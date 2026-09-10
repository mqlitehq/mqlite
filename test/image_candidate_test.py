#!/usr/bin/env python3
"""Negative controls for the actual multi-platform image publishing boundary."""
import copy
from contextlib import contextmanager
from datetime import date, datetime, timedelta
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("candidate", ROOT / ".github/scripts/image_candidate.py")
candidate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(candidate)
SMOKE_SPEC = importlib.util.spec_from_file_location("release_smoke", ROOT / "test/release_image_smoke.py")
release_smoke = importlib.util.module_from_spec(SMOKE_SPEC)
SMOKE_SPEC.loader.exec_module(release_smoke)
SHA = "1" * 40
SCAN_DATE = date(2026, 9, 10)


def build_info(arch):
    return ("fixture-mqlite: go1.27.1\n\tpath\tgithub.com/mqlitehq/mqlite/cmd/mqlite\n"
            "\tbuild\tGOOS=linux\n\tbuild\tGOARCH=" + arch + "\n\tbuild\tCGO_ENABLED=0\n")


def fixture(path, arches=("amd64", "arm64"), mutate=None):
    files = {}

    def blob(value, media_type):
        raw = value if isinstance(value, bytes) else json.dumps(value).encode()
        digest = candidate.digest(raw)
        files["blobs/sha256/" + digest] = raw
        return {"digest": "sha256:" + digest, "size": len(raw), "mediaType": media_type}

    manifests = []
    for arch in arches:
        config = {"architecture": arch, "os": "linux", "rootfs": {"diff_ids": []}, "config": {"Labels": {
            "org.opencontainers.image.source": candidate.SOURCE,
            "org.opencontainers.image.revision": SHA,
            "org.opencontainers.image.version": "0.3.0",
        }}}
        if mutate:
            mutate(config)
        item = blob({"config": blob(config, "application/vnd.oci.image.config.v1+json"),
                     "layers": [blob(b"layer content", "application/vnd.oci.image.layer.v1.tar")]},
                    "application/vnd.oci.image.manifest.v1+json")
        item["platform"] = {"os": "linux", "architecture": arch}
        manifests.append(item)
    root = blob({"schemaVersion": 2, "manifests": manifests}, "application/vnd.oci.image.index.v1+json")
    files["index.json"] = json.dumps({"schemaVersion": 2, "manifests": [root]}).encode()
    with tarfile.open(path, "w") as archive:
        for name, raw in files.items():
            member = tarfile.TarInfo(name)
            member.size = len(raw)
            archive.addfile(member, io.BytesIO(raw))
    return root


class CandidateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.archive = self.root / "image.tar"
        self.output = self.root / "checks"
        self.output.mkdir()
        fixture(self.archive)

    @contextmanager
    def verify_commands(self, output, failure=None, failed_arch=None):
        identity = candidate.inspect_archive(self.archive, "0.3.0", SHA)
        download = io.BytesIO()
        with tarfile.open(fileobj=download, mode="w:gz") as archive:
            member = tarfile.TarInfo("trivy")
            member.size = len(b"fixture scanner")
            archive.addfile(member, io.BytesIO(b"fixture scanner"))
        download_bytes = download.getvalue()
        events = {"loaded": [], "removed": [], "scans": [], "smokes": []}
        images = {}
        containers = {}

        def run(*args, **kwargs):
            self.assertFalse((output / "verified.json").exists(), "PASS was written before gates finished")
            if args[:1] == ("skopeo",) and "copy" in args:
                arch = args[args.index("--override-arch") + 1]
                destination, image = args[-1].removeprefix("docker-archive:").split(":", 1)
                images[image] = arch
                with tarfile.open(self.archive) as original:
                    raw = original.extractfile("blobs/sha256/" + identity["images"][arch]["config_digest"][7:]).read()
                files = {"config.json": raw, "manifest.json": json.dumps([
                    {"Config": "config.json", "RepoTags": [image], "Layers": []}]).encode()}
                with tarfile.open(destination, "w") as archive:
                    for name, value in files.items():
                        member = tarfile.TarInfo(name)
                        member.size = len(value)
                        archive.addfile(member, io.BytesIO(value))
            elif args[:1] == (str(output / "trivy"),):
                arch = args[args.index("--platform") + 1].split("/")[1]
                events["scans"].append(arch)
                report = {"SchemaVersion": 2, "Metadata": {"OS": {"Family": "alpine", "Name": "3.24.1"},
                          "ImageID": identity["images"][arch]["config_digest"],
                          "ImageConfig": identity["images"][arch]["config"]},
                          "Results": [{"Class": "os-pkgs", "Type": "alpine",
                                       "Packages": [{"Name": "libssl3"}]}]}
                if arch == failed_arch and failure in ("scan-command", "scan-report"):
                    report["Results"][0]["Vulnerabilities"] = [{"Severity": "HIGH"}]
                Path(args[args.index("--output") + 1]).write_text(json.dumps(report))
                if arch == failed_arch and failure == "scan-command":
                    raise subprocess.CalledProcessError(1, args)
            elif args[:2] == ("docker", "load"):
                with tarfile.open(args[-1]) as archive:
                    image = json.load(archive.extractfile("manifest.json"))[0]["RepoTags"][0]
                events["loaded"].append(image)
            elif args[:3] == ("docker", "image", "inspect"):
                arch = images[args[-1]]
                expected = identity["images"][arch]
                return json.dumps([{"Id": expected["config_digest"], "Os": "linux", "Architecture": arch,
                                    "Config": expected["config"]["config"],
                                    "RootFS": {"Layers": expected["config"]["rootfs"]["diff_ids"]}}]).encode()
            elif args[:2] == ("docker", "create"):
                arch = args[args.index("--platform") + 1].split("/")[1]
                self.assertEqual(args[-1], identity["images"][arch]["config_digest"])
                containers[args[args.index("--name") + 1]] = arch
            elif args[:2] == ("docker", "cp"):
                arch = containers[args[-2].split(":", 1)[0]]
                Path(args[-1]).write_bytes(b"fixture binary: " + arch.encode())
            elif args[:2] == ("docker", "rm"):
                del containers[args[-1]]
            elif args[:3] == ("docker", "image", "rm"):
                self.assertIn(args[-1], events["loaded"])
                events["removed"].append(args[-1])
            elif args[:3] == ("go", "version", "-m"):
                arch = Path(args[-1]).name.removesuffix("-mqlite")
                if arch == failed_arch and failure == "build-info":
                    arch = "arm64" if arch == "amd64" else "amd64"
                return build_info(arch).encode()
            elif len(args) > 1 and args[1] == str(ROOT / "test/release_image_smoke.py"):
                arch = args[args.index("--platform") + 1].split("/")[1]
                self.assertEqual(args[2], identity["images"][arch]["config_digest"])
                events["smokes"].append(arch)
                failed = arch == failed_arch and failure == "smoke"
                kwargs["log"].write_text("FAIL fixture smoke\n" if failed else "PASS fixture smoke\n")
                if failed:
                    raise subprocess.CalledProcessError(1, args)
            else:
                self.fail("unexpected external command: " + repr(args))

        with patch.object(candidate.urllib.request, "urlopen", return_value=io.BytesIO(download_bytes)), \
                patch.object(candidate, "TRIVY_SHA256", candidate.digest(download_bytes)), \
                patch.object(candidate, "command", side_effect=run), patch("builtins.print"), \
                patch.object(candidate, "datetime") as clock:
            clock.now.return_value = datetime.combine(SCAN_DATE, datetime.min.time(), candidate.timezone.utc)
            yield identity, events
        self.assertFalse(containers, "binary extraction container was not cleaned up")

    def test_verify_pass_requires_both_platforms_and_complete_evidence(self):
        output = self.root / "verify-pass"
        with self.verify_commands(output) as (identity, events):
            candidate.verify(self.archive, output, "0.3.0", SHA)
        self.assertEqual(json.loads((output / "verified.json").read_text()), dict(identity, status="PASS"))
        self.assertFalse((output / "verified.json.tmp").exists())
        binaries = json.loads((output / "binaries.json").read_text())
        self.assertEqual(set(binaries), {"amd64", "arm64"})
        for arch in ("amd64", "arm64"):
            self.assertEqual(binaries[arch]["binary_sha256"], candidate.digest(b"fixture binary: " + arch.encode()))
            self.assertTrue((output / (arch + "-os-scan.json")).is_file())
            self.assertEqual((output / (arch + "-smoke.log")).read_text(), "PASS fixture smoke\n")
        self.assertEqual(events["scans"], ["amd64", "arm64"])
        self.assertEqual(events["smokes"], ["amd64", "arm64"])
        self.assertEqual(len(events["loaded"]), 2)
        self.assertEqual(events["removed"], events["loaded"])

    def test_verify_gate_failures_never_write_pass_and_clean_loaded_images(self):
        for failure in ("scan-command", "scan-report", "build-info", "smoke"):
            for arch in ("amd64", "arm64"):
                with self.subTest(failure=failure, arch=arch):
                    output = self.root / (failure + "-" + arch)
                    with self.verify_commands(output, failure, arch) as (_, events):
                        expected_error = RuntimeError if failure in ("scan-report", "build-info") else subprocess.CalledProcessError
                        with self.assertRaises(expected_error):
                            candidate.verify(self.archive, output, "0.3.0", SHA)
                    self.assertFalse((output / "verified.json").exists())
                    self.assertFalse((output / "verified.json.tmp").exists())
                    self.assertEqual(events["scans"], ["amd64"] if arch == "amd64" else ["amd64", "arm64"])
                    completed = [] if arch == "amd64" else ["amd64"]
                    expected_smokes = completed + ([arch] if failure == "smoke" else [])
                    self.assertEqual(events["smokes"], expected_smokes)
                    expected_loaded = len(completed) + (1 if failure in ("build-info", "smoke") else 0)
                    self.assertEqual(len(events["loaded"]), expected_loaded)
                    self.assertEqual(events["removed"], events["loaded"])

    def test_binary_build_info_requires_each_setting_once_for_each_platform(self):
        for arch in ("amd64", "arm64"):
            valid = build_info(arch)
            candidate.check_build_info(valid, arch)
            fields = (("path", "github.com/mqlitehq/mqlite/cmd/mqlite", "example.com/wrong"),
                      ("GOOS", "linux", "windows"),
                      ("GOARCH", arch, "arm64" if arch == "amd64" else "amd64"),
                      ("CGO_ENABLED", "0", "1"))
            for key, expected, wrong in fields:
                line = "\tpath\t" + expected + "\n" if key == "path" else "\tbuild\t" + key + "=" + expected + "\n"
                for mutation, changed in (("missing", valid.replace(line, "")),
                                          ("duplicate", valid + line),
                                          ("wrong", valid.replace(line, line.replace(expected, wrong)))):
                    with self.subTest(arch=arch, key=key, mutation=mutation):
                        with self.assertRaises(RuntimeError):
                            candidate.check_build_info(changed, arch)

    def test_verify_binary_evidence_write_failure_cannot_leave_pass(self):
        output = self.root / "verify-write-failure"
        write_text = Path.write_text

        def fail_binary_evidence(path, *args, **kwargs):
            if path == output / "binaries.json":
                raise OSError("fixture disk full writing binaries.json")
            return write_text(path, *args, **kwargs)

        with self.verify_commands(output) as (_, events), \
                patch.object(Path, "write_text", fail_binary_evidence):
            with self.assertRaisesRegex(OSError, "disk full writing binaries.json"):
                candidate.verify(self.archive, output, "0.3.0", SHA)
        self.assertEqual(events["smokes"], ["amd64", "arm64"])
        self.assertEqual(len(events["loaded"]), 2)
        self.assertEqual(events["removed"], events["loaded"])
        self.assertFalse((output / "verified.json").exists())
        self.assertFalse((output / "verified.json.tmp").exists())

    def test_smoke_run_timeout_hides_generated_token_and_cleans_resources(self):
        calls = []
        args = SimpleNamespace(docker="docker", image="fixture-image", platform="linux/arm64",
                               expected_version="0.3.0", expected_revision=SHA)
        labels = {"org.opencontainers.image." + name: value for name, value in (
            ("source", candidate.SOURCE), ("version", "0.3.0"), ("revision", SHA))}

        def run(argv, **kwargs):
            calls.append(argv)
            stdout = ""
            if argv[1:3] == ["image", "inspect"]:
                stdout = json.dumps([{"Os": "linux", "Architecture": "arm64", "Config": {"Labels": labels}}])
            elif argv[1] == "run":
                raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
            elif argv[1:3] != ["volume", "create"] and argv[1] not in ("logs", "rm", "volume"):
                self.fail("unexpected smoke command: " + repr(argv))
            return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")

        with patch.object(release_smoke.subprocess, "run", side_effect=run), patch("builtins.print") as output:
            with self.assertRaisesRegex(RuntimeError, "docker run timed out after 60 seconds") as failure:
                release_smoke.smoke(args)
        self.assertNotIn("mqk_", str(failure.exception))
        self.assertNotIn("MQLITE_TOKENS", str(failure.exception))
        self.assertTrue(failure.exception.__suppress_context__)
        printed = " ".join(str(call.args) for call in output.call_args_list)
        self.assertNotIn("mqk_", printed)
        self.assertNotIn("MQLITE_TOKENS", printed)
        started = next(call for call in calls if call[1] == "run")
        self.assertTrue(any(value.startswith("MQLITE_TOKENS=mqk_") for value in started))
        container = started[started.index("--name") + 1]
        volume = next(call[-1] for call in calls if call[1:3] == ["volume", "create"])
        self.assertIn(["docker", "rm", "--force", container], calls)
        self.assertIn(["docker", "volume", "rm", "--force", volume], calls)

    def test_complete_oci_inventory_and_every_provenance_field(self):
        actual = candidate.inspect_archive(self.archive, "0.3.0", SHA)
        self.assertEqual(set(actual["images"]), {"amd64", "arm64"})
        for arches in (("amd64",), ("arm64",), ("amd64", "amd64"), ("amd64", "arm64", "riscv64"), ()):
            with self.subTest(arches=arches):
                fixture(self.archive, arches)
                with self.assertRaises(RuntimeError):
                    candidate.inspect_archive(self.archive, "0.3.0", SHA)
        for field in ("source", "version", "revision"):
            with self.subTest(field=field):
                fixture(self.archive, mutate=lambda cfg: cfg["config"]["Labels"].update({
                    "org.opencontainers.image." + field: "wrong"}))
                with self.assertRaises(RuntimeError):
                    candidate.inspect_archive(self.archive, "0.3.0", SHA)
        for field, value in (("architecture", "riscv64"), ("os", "windows")):
            fixture(self.archive, mutate=lambda cfg: cfg.update({field: value}))
            with self.assertRaises(RuntimeError):
                candidate.inspect_archive(self.archive, "0.3.0", SHA)

    def test_rehashed_graph_and_corrupted_blob_are_distinct_failures(self):
        raw = self.archive.read_bytes()
        self.assertIn(b"layer content", raw)
        self.archive.write_bytes(raw.replace(b"layer content", b"LAYER CONTENT"))
        with self.assertRaisesRegex(RuntimeError, "blob size/digest"):
            candidate.inspect_archive(self.archive, "0.3.0", SHA)

    def test_unknown_empty_wrong_platform_and_vulnerable_scans_fail_closed(self):
        clean = {"SchemaVersion": 2, "Metadata": {"OS": {"Family": "alpine", "Name": "3.24.1"},
                 "ImageID": "sha256:config", "ImageConfig": {"architecture": "arm64", "os": "linux"}},
                 "Results": [{"Class": "os-pkgs", "Type": "alpine", "Packages": [{"Name": "libssl3"}]}]}
        candidate.check_scan(clean, "arm64", "sha256:config", today=SCAN_DATE)
        mutations = [
            lambda r: r.update(SchemaVersion=1),
            lambda r: r["Metadata"]["OS"].update(Family="unknown"),
            lambda r: r["Metadata"]["OS"].update(Name=""),
            lambda r: r["Metadata"]["OS"].update(Name="3.23.0"),
            lambda r: r["Metadata"]["OS"].update(Name="3.25.0"),
            lambda r: r["Metadata"]["OS"].update(EOSL=True),
            lambda r: r["Metadata"].update(ImageID="sha256:other"),
            lambda r: r["Metadata"]["ImageConfig"].update(architecture="amd64"),
            lambda r: r["Metadata"]["ImageConfig"].update(os="windows"),
            lambda r: r.update(Results=[]),
            lambda r: r["Results"][0].update(Packages=[]),
            lambda r: r["Results"][0].update(Class="lang-pkgs"),
            lambda r: r["Results"][0].update(Type="other"),
        ]
        for severity in ("HIGH", "CRITICAL"):
            bad = copy.deepcopy(clean)
            bad["Results"][0]["Vulnerabilities"] = [{"Severity": severity}]
            with self.assertRaises(RuntimeError):
                candidate.check_scan(bad, "arm64", "sha256:config", today=SCAN_DATE)
        for mutation in mutations:
            bad = copy.deepcopy(clean)
            mutation(bad)
            with self.assertRaises(RuntimeError):
                candidate.check_scan(bad, "arm64", "sha256:config", today=SCAN_DATE)
        candidate.check_scan(clean, "arm64", "sha256:config",
                             today=candidate.ALPINE_END_OF_SUPPORT - timedelta(days=1))
        for today in (candidate.ALPINE_END_OF_SUPPORT, candidate.ALPINE_END_OF_SUPPORT + timedelta(days=1)):
            with self.subTest(today=today):
                with self.assertRaisesRegex(RuntimeError, "no longer supported"):
                    candidate.check_scan(clean, "arm64", "sha256:config", today=today)

    def test_no_publish_after_missing_failed_or_changed_candidate(self):
        identity = candidate.inspect_archive(self.archive, "0.3.0", SHA)
        for status, change in ((None, False), ("FAIL", False), ("PASS", True)):
            if status:
                evidence = dict(identity, status=status)
                if change:
                    evidence["archive_sha256"] = "0" * 64
                (self.output / "verified.json").write_text(json.dumps(evidence))
            with patch.object(candidate, "command") as run:
                with self.assertRaises((RuntimeError, FileNotFoundError)):
                    candidate.publish(self.archive, self.output, "0.3.0", SHA, "")
                run.assert_not_called()

    def test_stable_and_rc_publish_only_the_original_index_and_allowed_tags(self):
        identity = candidate.inspect_archive(self.archive, "0.3.0", SHA)
        with tarfile.open(self.archive) as archive:
            raw_index = archive.extractfile("blobs/sha256/" + identity["index_digest"][7:]).read()
        image = "ghcr.io/mqlitehq/mqlite"
        for version, tags in (("0.3.0", [image + ":0.3.0", image + ":0.3", image + ":latest"]),
                              ("0.3.0-rc.1", [image + ":0.3.0-rc.1"])):
            identity["version"] = version
            (self.output / "verified.json").write_text(json.dumps(dict(identity, status="PASS")))
            with patch.dict("os.environ", GITHUB_REPOSITORY="mqlitehq/mqlite"), \
                    patch.object(candidate, "inspect_archive", return_value=identity), \
                    patch("builtins.print"), \
                    patch.object(candidate, "command", return_value=raw_index) as run:
                candidate.publish(self.archive, self.output, version, SHA, ",".join(tags))
                self.assertEqual(run.call_count, len(tags) * 2)
                for index, tag in enumerate(tags):
                    self.assertEqual(run.call_args_list[index * 2].args,
                                     ("skopeo", "copy", "--all", "--preserve-digests",
                                      "oci-archive:" + str(self.archive), "docker://" + tag))
                for bad in ([], tags + [image + ":extra"], [image + ":latest"], tags * 2):
                    run.reset_mock()
                    with self.assertRaises(RuntimeError):
                        candidate.publish(self.archive, self.output, version, SHA, ",".join(bad))
                    run.assert_not_called()
                run.return_value = b"changed remote index"
                with self.assertRaisesRegex(RuntimeError, "published index digest mismatch"):
                    candidate.publish(self.archive, self.output, version, SHA, ",".join(tags))


if __name__ == "__main__":
    unittest.main()
