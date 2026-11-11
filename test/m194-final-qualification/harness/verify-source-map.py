#!/usr/bin/env python3
"""Verify reviewed product/SDK bytes without consulting database configuration."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

HARNESS_ROOT = "test/m163-shutdown-clients"
EXPECTED_NAME = "source-map.expected.json"
SDK_ROOT = ".diagnostic-sdk"
WORKFLOW = ".github/workflows/m163-shutdown-clients.yml"


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args], text=True).strip()


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def names(root, *args):
    raw = subprocess.check_output(["git", "-C", str(root), *args, "-z"])
    return {os.fsdecode(name) for name in raw.split(b"\0") if name}


def checked_mapping(value):
    if not isinstance(value, dict) or not value:
        raise RuntimeError("source manifest is not a nonempty mapping")
    for name, checksum in value.items():
        if (not isinstance(name, str) or not name or "\\" in name or
                name.startswith("/") or any(part in ("", ".", "..", ".git", SDK_ROOT) for part in name.split("/")) or
                any(ord(c) < 32 for c in name) or not isinstance(checksum, str) or
                re.fullmatch(r"[0-9a-f]{64}", checksum) is None):
            raise RuntimeError("invalid source manifest path or checksum")
    return value


def require_clean(root, allow_sdk=False):
    # Do not restrict this to runtime..HEAD: the index and worktree can add
    # compilation inputs that do not appear in a committed diagnostic diff.
    status = subprocess.check_output(["git", "-C", str(root), "status", "--porcelain=v1", "-z", "--untracked-files=all"])
    for record in status.split(b"\0"):
        if not record:
            continue
        if allow_sdk and record[:3] == b"?? " and os.fsdecode(record[3:]).rstrip("/") == SDK_ROOT:
            continue
        raise RuntimeError("source worktree or index is not clean")
    if subprocess.run(["git", "-C", str(root), "diff", "--quiet", "HEAD", "--"], check=False).returncode:
        raise RuntimeError("source worktree or index differs from HEAD")


def filesystem_files(root, skip_sdk=False):
    # Enumerate actual files, including ignored ones, instead of relying on
    # git status to discover inputs. Never follow a symlink or accept a socket.
    result = set()

    def walk(directory):
        for path in directory.iterdir():
            relative = path.relative_to(root).as_posix()
            if path.is_symlink():
                raise RuntimeError("source tree contains a symlink")
            if relative == ".git" or skip_sdk and relative == SDK_ROOT:
                continue
            if path.is_dir():
                walk(path)
            elif path.is_file():
                result.add(relative)
            else:
                raise RuntimeError("source tree contains a non-regular input")

    walk(root)
    return result


def verify_head_blobs(root):
    # Index flags can hide a changed working file from git status. Exact HEAD
    # plus a clean status is not by itself an exact-byte source proof.
    object_format = git(root, "rev-parse", "--show-object-format")
    if object_format not in ("sha1", "sha256"):
        raise RuntimeError("unsupported Git object format")
    entries = subprocess.check_output(["git", "-C", str(root), "ls-tree", "-r", "-z", "HEAD"])
    for entry in entries.split(b"\0"):
        if not entry:
            continue
        header, name = entry.split(b"\t", 1)
        mode, kind, checksum = header.decode("ascii").split()
        if kind != "blob" or mode not in ("100644", "100755"):
            raise RuntimeError("source includes a non-regular tracked input")
        data = (root / os.fsdecode(name)).read_bytes()
        actual = hashlib.new(object_format, b"blob " + str(len(data)).encode() + b"\0" + data).hexdigest()
        if actual != checksum:
            raise RuntimeError("working bytes differ from the exact reviewed HEAD")


def verify(workspace, phase):
    workspace = Path(workspace).absolute()
    if workspace.is_symlink() or not workspace.is_dir():
        raise RuntimeError("workspace must be a real directory")
    workspace = workspace.resolve()
    support = workspace / HARNESS_ROOT
    if Path(__file__).absolute() != support / "verify-source-map.py":
        raise RuntimeError("source verifier is outside the reviewed harness")
    for path in (workspace / "test", support, support / "verify-source-map.py", support / EXPECTED_NAME):
        if path.is_symlink():
            raise RuntimeError("harness path contains a symlink")
    expected = json.loads((support / EXPECTED_NAME).read_text())
    runtime_files = checked_mapping(expected["runtime_files"])
    harness_files = checked_mapping(expected["harness_files"])
    if EXPECTED_NAME in harness_files or WORKFLOW in runtime_files:
        raise RuntimeError("source manifest has an invalid inventory")
    harness_digest = hashlib.sha256(json.dumps(harness_files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    if harness_digest != expected["harness_sha256"]:
        raise RuntimeError("harness manifest differs from its pinned digest")
    runtime = expected["runtime_sha"]
    sdk = workspace / SDK_ROOT
    if sdk.is_symlink() or not sdk.is_dir() or git(sdk, "rev-parse", "HEAD") != expected["sdk_sha"]:
        raise RuntimeError("SDK source identity mismatch")
    require_clean(sdk)
    sdk_tracked = names(sdk, "ls-files")
    if filesystem_files(sdk) != sdk_tracked:
        raise RuntimeError("SDK filesystem inventory changed")
    verify_head_blobs(sdk)
    require_clean(workspace, allow_sdk=True)
    if digest(workspace / WORKFLOW) != expected["workflow_sha256"]:
        raise RuntimeError("reviewed diagnostic workflow changed")
    subprocess.run(["git", "-C", str(workspace), "merge-base", "--is-ancestor", runtime, "HEAD"], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    allowed = {WORKFLOW, HARNESS_ROOT + "/" + EXPECTED_NAME}
    allowed.update(HARNESS_ROOT + "/" + name for name in harness_files)
    changed = names(workspace, "diff", runtime, "--name-only", "HEAD")
    if not changed.issubset(allowed):
        raise RuntimeError("diagnostic branch changed product runtime")
    expected_tracked = set(runtime_files) | allowed
    if names(workspace, "ls-files") != expected_tracked:
        raise RuntimeError("tracked diagnostic inventory differs from the reviewed files")
    actual_files = filesystem_files(workspace, skip_sdk=True)
    if actual_files != expected_tracked:
        raise RuntimeError("actual diagnostic inputs differ from the reviewed inventory")
    actual_harness = filesystem_files(support)
    if actual_harness != set(harness_files) | {EXPECTED_NAME}:
        raise RuntimeError("actual harness inventory differs from the reviewed files")
    verify_head_blobs(workspace)
    for name, value in runtime_files.items():
        if name == WORKFLOW:
            continue
        path = workspace / name
        if path.is_symlink() or digest(path) != value:
            raise RuntimeError("runtime source bytes changed")
    for name, value in harness_files.items():
        if digest(support / name) != value:
            raise RuntimeError("harness source bytes changed")
    for name, value in (("M164_RUNTIME_SHA", runtime), ("M164_SDK_SHA", expected["sdk_sha"]),
                        ("M164_HARNESS_SHA", expected["harness_sha256"])):
        if os.environ.get(name) != value:
            raise RuntimeError("declared source identity changed")
    result = {"runtime_sha": runtime, "sdk_sha": expected["sdk_sha"],
              "diagnostic_sha": git(workspace, "rev-parse", "HEAD"),
              "harness_sha256": expected["harness_sha256"],
              "workflow_sha256":expected["workflow_sha256"],
              "runtime_files": expected["runtime_files"], "harness_files": expected["harness_files"],
              "product_and_sdk_verified": True, "phase": phase,
              "root_worktree_and_index_clean": True, "root_tracked_inventory_exact": True,
              "root_filesystem_inventory_exact": True, "harness_filesystem_inventory_exact": True,
              "sdk_filesystem_inventory_exact": True, "harness_manifest_digest_verified": True,
              "sdk_head_blob_bytes_verified": True,
              "runtime_tree": git(workspace, "rev-parse", runtime + "^{tree}"),
              "sdk_tree": git(sdk, "rev-parse", "HEAD^{tree}"),
              "sdk_files": {name: digest(sdk / name) for name in sorted(sdk_tracked)},
              "symlinks_rejected": True}
    result["go_version"] = subprocess.check_output(["go", "version"], text=True).strip()
    output = Path(os.environ["M164_EVIDENCE_DIR"])
    if output.resolve().is_relative_to(workspace):
        raise RuntimeError("evidence must be outside the reviewed checkout")
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = output / ("source-map-" + phase + ".json")
    if target.exists() or target.is_symlink():
        raise RuntimeError("refusing to overwrite source verification evidence")
    with target.open("x") as file:
        file.write(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--workspace", type=Path, required=True)
    parser.add_argument("--phase", choices=("before", "after"), required=True)
    args = parser.parse_args()
    try:
        verify(args.workspace, args.phase)
    except BaseException:
        print("Diagnostic source-map verification failed.", file=sys.stderr)
        sys.exit(1)
    print("Diagnostic source-map verification passed.")
