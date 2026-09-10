#!/usr/bin/env python3
"""Verify both platforms of one OCI archive, then publish those same bytes."""
import argparse
from datetime import date, datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tarfile
import urllib.request
import uuid


ROOT = Path(__file__).resolve().parents[2]
PLATFORMS = ("amd64", "arm64")
TRIVY_VERSION = "0.74.0"
TRIVY_SHA256 = "2ae6fe3ee734b7fdf11335663e18c75ea12dccc76062f09f164a3b0f8be4371a"
SOURCE = "https://github.com/mqlitehq/mqlite"
# Alpine 3.24 main support: https://alpinelinux.org/releases/ (reviewed 2026-09-10).
# Trivy 0.74 can scan this branch but does not yet know its support deadline.
ALPINE_END_OF_SUPPORT = date(2028, 6, 1)


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def file_digest(path):
    value = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            value.update(block)
    return value.hexdigest()


def command(*args, capture=False, log=None):
    if log is not None:
        try:
            with log.open("w") as output:
                subprocess.run(args, cwd=ROOT, check=True, stdout=output, stderr=subprocess.STDOUT)
        finally:
            print(log.read_text(), end="", flush=True)
        return None
    result = subprocess.run(args, cwd=ROOT, check=True, stdout=subprocess.PIPE if capture else None)
    return result.stdout if capture else None


def inspect_archive(archive, version, revision):
    """Check the complete OCI graph and require exactly the two shipped platforms."""
    with tarfile.open(archive) as bundle:
        def blob(descriptor):
            value = descriptor["digest"]
            require(re.fullmatch(r"sha256:[0-9a-f]{64}", value), "invalid OCI digest")
            raw = bundle.extractfile("blobs/sha256/" + value[7:]).read()
            require(len(raw) == descriptor["size"] and "sha256:" + digest(raw) == value,
                    "OCI blob size/digest mismatch")
            return raw

        outer = json.load(bundle.extractfile("index.json"))
        require(len(outer["manifests"]) == 1, "OCI archive must have one root index")
        root = outer["manifests"][0]
        index = json.loads(blob(root))
        require(root["mediaType"] == "application/vnd.oci.image.index.v1+json",
                "OCI root must be a multi-platform index")
        images = {}
        for descriptor in index["manifests"]:
            platform = descriptor["platform"]
            arch = platform["architecture"]
            require(platform["os"] == "linux" and arch in PLATFORMS and arch not in images,
                    "unexpected or duplicate OCI platform")
            manifest = json.loads(blob(descriptor))
            config = json.loads(blob(manifest["config"]))
            require(config["os"] == "linux" and config["architecture"] == arch,
                    "OCI config platform mismatch")
            labels = config["config"]["Labels"]
            for name, expected in (("source", SOURCE), ("version", version), ("revision", revision)):
                require(labels.get("org.opencontainers.image." + name) == expected,
                        "OCI " + name + " mismatch")
            for layer in manifest["layers"]:
                blob(layer)
            images[arch] = {"manifest_digest": descriptor["digest"],
                            "config_digest": manifest["config"]["digest"], "config": config}
        require(set(images) == set(PLATFORMS), "OCI platform inventory mismatch")
        return {"archive_sha256": file_digest(archive), "index_digest": root["digest"],
                "version": version, "revision": revision, "images": images}


def check_scan(report, arch, config_digest, today=None):
    require(report.get("SchemaVersion") == 2, "unsupported vulnerability report")
    metadata = report["Metadata"]
    require(metadata["OS"]["Family"] == "alpine" and not metadata["OS"].get("EOSL", False),
            "missing Alpine scan or end-of-life OS")
    require(re.fullmatch(r"3\.24\.(0|[1-9][0-9]*)", metadata["OS"].get("Name", ""))
            and (today or datetime.now(timezone.utc).date()) < ALPINE_END_OF_SUPPORT,
            "runtime Alpine branch is unreviewed or no longer supported")
    require(metadata["ImageID"] == config_digest, "scan image config digest mismatch")
    require(metadata["ImageConfig"]["architecture"] == arch
            and metadata["ImageConfig"]["os"] == "linux", "scan platform mismatch")
    results = report["Results"]
    require(any(r.get("Class") == "os-pkgs" and r.get("Type") == "alpine"
                and r.get("Packages") for r in results), "OS package inventory missing")
    require(not any(v.get("Severity") in ("HIGH", "CRITICAL")
                    for r in results for v in (r.get("Vulnerabilities") or [])),
            "HIGH/CRITICAL OS vulnerability")


def check_build_info(info, arch):
    # Image labels alone cannot prove the executable's architecture.
    for key, expected in (("GOOS", "linux"), ("GOARCH", arch), ("CGO_ENABLED", "0")):
        values = re.findall(r"^\s*build\s+" + key + r"=(\S+)\s*$", info, re.MULTILINE)
        require(values == [expected], "binary build setting mismatch: " + key)
    paths = re.findall(r"^\s*path\s+(\S+)\s*$", info, re.MULTILINE)
    require(paths == ["github.com/mqlitehq/mqlite/cmd/mqlite"], "unexpected binary build path")


def verify(archive, output, version, revision):
    output.mkdir(parents=True, exist_ok=False)
    identity = inspect_archive(archive, version, revision)
    trivy_archive = output / "trivy.tar.gz"
    url = ("https://github.com/aquasecurity/trivy/releases/download/v" + TRIVY_VERSION
           + "/trivy_" + TRIVY_VERSION + "_Linux-64bit.tar.gz")
    with urllib.request.urlopen(url, timeout=60) as response, trivy_archive.open("wb") as target:
        while block := response.read(1 << 20):
            target.write(block)
    require(file_digest(trivy_archive) == TRIVY_SHA256, "Trivy download checksum mismatch")
    trivy = output / "trivy"
    with tarfile.open(trivy_archive) as bundle:
        trivy.write_bytes(bundle.extractfile("trivy").read())
    trivy.chmod(0o755)
    (output / "config.yaml").write_text("{}\n")
    (output / "empty-ignore").write_text("")
    binaries = {}
    for arch in PLATFORMS:
        image = "mqlite-gate:" + uuid.uuid4().hex + "-" + arch
        exported = output / (arch + ".tar")
        command("skopeo", "--override-os", "linux", "--override-arch", arch, "copy",
                "oci-archive:" + str(archive), "docker-archive:" + str(exported) + ":" + image)
        # Docker archive conversion changes compressed layer manifests, but not the config.
        with tarfile.open(exported) as bundle:
            entries = json.load(bundle.extractfile("manifest.json"))
            require(len(entries) == 1, "exported archive must contain one image")
            raw = bundle.extractfile(entries[0]["Config"]).read()
            require("sha256:" + digest(raw) == identity["images"][arch]["config_digest"],
                    "exported image differs from OCI source")
        report_path = output / (arch + "-os-scan.json")
        command(str(trivy), "image", "--input", str(exported), "--platform", "linux/" + arch,
                "--config", str(output / "config.yaml"), "--ignorefile", str(output / "empty-ignore"),
                "--cache-dir", str(output / "cache"), "--db-repository", "ghcr.io/aquasecurity/trivy-db:2",
                "--scanners", "vuln", "--pkg-types", "os", "--severity", "HIGH,CRITICAL",
                "--exit-code", "1", "--exit-on-eol", "1", "--list-all-pkgs",
                "--skip-java-db-update", "--no-progress", "--format", "json", "--output", str(report_path))
        check_scan(json.loads(report_path.read_text()), arch, identity["images"][arch]["config_digest"])
        loaded = False
        try:
            command("docker", "load", "--input", str(exported))
            loaded = True
            actual = json.loads(command("docker", "image", "inspect", image, capture=True))[0]
            expected = identity["images"][arch]["config"]
            require(actual["Config"] == expected["config"]
                    and actual["RootFS"]["Layers"] == expected["rootfs"]["diff_ids"]
                    and actual["Os"] == "linux" and actual["Architecture"] == arch,
                    "loaded image differs from scanned OCI source")
            container = "mqlite-gate-binary-" + uuid.uuid4().hex
            binary = output / (arch + "-mqlite")
            command("docker", "create", "--platform", "linux/" + arch, "--name", container, actual["Id"])
            try:
                command("docker", "cp", container + ":/usr/local/bin/mqlite", str(binary))
            finally:
                command("docker", "rm", "--volumes", container)
            build_info = command("go", "version", "-m", str(binary), capture=True).decode()
            check_build_info(build_info, arch)
            binaries[arch] = {"image_id": actual["Id"], "binary_sha256": file_digest(binary),
                              "go_build_info": build_info}
            binary.unlink()
            command(sys.executable, str(ROOT / "test/release_image_smoke.py"), actual["Id"],
                    "--platform", "linux/" + arch, "--expected-version", version,
                    "--expected-revision", revision, log=output / (arch + "-smoke.log"))
        finally:
            if loaded:
                command("docker", "image", "rm", image)
        exported.unlink()
    require(file_digest(archive) == identity["archive_sha256"], "OCI archive changed during verification")
    (output / "binaries.json").write_text(json.dumps(binaries, indent=2) + "\n")
    identity["status"] = "PASS"
    marker = output / "verified.json.tmp"
    marker.write_text(json.dumps(identity, indent=2) + "\n")
    marker.replace(output / "verified.json")
    print("PASS both platforms: OS scan, authenticated flow, persistent restart; " + identity["index_digest"])


def publish(archive, output, version, revision, tags):
    approved = json.loads((output / "verified.json").read_text())
    require(approved.pop("status") == "PASS", "candidate verification did not pass")
    require(approved == inspect_archive(archive, version, revision), "verified OCI candidate changed")
    image = "ghcr.io/" + os.environ["GITHUB_REPOSITORY"].lower()
    expected = [image + ":" + version]
    if "-rc." not in version:
        expected += [image + ":" + ".".join(version.split(".")[:2]), image + ":latest"]
    require(tags.split(",") == expected, "publish tags differ from the release contract")
    for tag in expected:
        command("skopeo", "copy", "--all", "--preserve-digests", "oci-archive:" + str(archive),
                "docker://" + tag)
        raw = command("skopeo", "inspect", "--raw", "docker://" + tag, capture=True)
        require("sha256:" + digest(raw) == approved["index_digest"], "published index digest mismatch")
    print("PASS published verified index: " + approved["index_digest"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=("verify", "publish"))
    parser.add_argument("--archive", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--revision", required=True)
    parser.add_argument("--tags", default="")
    args = parser.parse_args()
    require(re.fullmatch(r"[0-9a-f]{40}", args.revision), "invalid source revision")
    require(re.fullmatch(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-rc\.[1-9][0-9]*)?", args.version),
            "invalid source version")
    inputs = (args.archive.resolve(), args.output.resolve(), args.version, args.revision)
    if args.action == "verify":
        verify(*inputs)
    else:
        publish(*inputs, args.tags)


if __name__ == "__main__":
    main()
