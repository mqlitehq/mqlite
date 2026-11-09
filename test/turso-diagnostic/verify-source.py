#!/usr/bin/env python3
"""Strict registered-branch before/after source proof; never reads DB settings."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

PRODUCT = "9591bc5f9b62017705404555f9ad5fa98ea497e0"
SDK = "93002d0d47e2e9e876e8b4f4c8a66c5167f29b10"
WORKFLOW = ".github/workflows/turso-nightly.yml"
GETTER = "engine/validation_access.go"
MANIFEST = "test/turso-diagnostic/source-map.expected.json"
TOOL_PREFIXES = ("test/turso-diagnostic/", "test/turso-protocol-baseline/", "test/turso-diagnostic-cleanup/")


def git(root, *args):
    return subprocess.check_output(["git", "-C", str(root), *args])


def names(root, *args):
    return {os.fsdecode(n) for n in git(root, *args, "-z").split(b"\0") if n}


def checksum(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def filesystem(root):
    found = set()

    def walk(folder):
        for p in folder.iterdir():
            name = p.relative_to(root).as_posix()
            if name == ".git":
                continue
            assert not p.is_symlink(), "source symlink"
            if p.is_dir():
                walk(p)
            else:
                assert p.is_file(), "nonregular source input"
                found.add(name)

    walk(root)
    return found


def clean_exact(root):
    assert not git(root, "status", "--porcelain=v1", "--untracked-files=all")
    assert subprocess.run(["git", "-C", str(root), "diff", "--quiet", "HEAD", "--"], check=False).returncode == 0
    tracked = names(root, "ls-files")
    assert tracked == filesystem(root), "extra/missing/ignored source input"
    for name in tracked:
        # This check also catches working bytes hidden by assume-unchanged.
        assert (root / name).read_bytes() == git(root, "show", "HEAD:" + name), "working bytes differ from HEAD"
    return tracked


def verify(source, sdk, phase, output):
    assert not source.is_symlink() and not sdk.is_symlink(), "source root symlink"
    source, sdk, output = source.resolve(), sdk.resolve(), output.resolve()
    assert source != sdk and not sdk.is_relative_to(source) and not source.is_relative_to(sdk)
    assert not output.is_relative_to(source) and not output.is_relative_to(sdk)
    diagnostic = git(source, "rev-parse", "HEAD").decode().strip()
    assert diagnostic == os.environ["EXPECTED_SOURCE"], "diagnostic revision mismatch"
    assert git(sdk, "rev-parse", "HEAD").decode().strip() == SDK
    subprocess.run(["git", "-C", str(source), "merge-base", "--is-ancestor", PRODUCT, "HEAD"], check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    root_files, sdk_files = clean_exact(source), clean_exact(sdk)
    base_files = names(source, "ls-tree", "-r", "--name-only", PRODUCT)
    manifest = json.loads((source / MANIFEST).read_text())
    assert manifest["runtime_sha"] == PRODUCT and manifest["sdk_sha"] == SDK
    tools = manifest["tool_files"]
    assert isinstance(tools, dict) and tools and MANIFEST not in tools
    for name, digest in tools.items():
        assert isinstance(name, str) and name and not name.startswith("/") and "\\" not in name
        assert all(part not in ("", ".", "..", ".git") for part in name.split("/"))
        assert name in (WORKFLOW, GETTER) or name.startswith(TOOL_PREFIXES), "tool inventory may not include product inputs"
        assert re.fullmatch(r"[0-9a-f]{64}", digest)
    assert WORKFLOW in tools and GETTER in tools
    assert root_files == base_files | set(tools) | {MANIFEST}, "registered branch inventory changed"
    assert names(source, "diff", "--name-only", PRODUCT, "HEAD").issubset(set(tools) | {MANIFEST})
    runtime_files = {}
    for name in sorted(base_files):
        data = git(source, "show", PRODUCT + ":" + name)
        if name != WORKFLOW:
            assert (source / name).read_bytes() == data, "product input changed"
        runtime_files[name] = hashlib.sha256(data).hexdigest()
    for name, digest in tools.items():
        assert checksum(source / name) == digest, "reviewed support changed"
    assert (source / GETTER).read_bytes().startswith(b"//go:build mqlite_perf_diag\n")
    harness_files = {name: digest for name, digest in tools.items() if name != WORKFLOW}
    harness_digest = hashlib.sha256(json.dumps(harness_files, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
    assert harness_digest == manifest["harness_sha256"] == os.environ["EXPECTED_HARNESS_SHA"]
    result = {"phase": phase, "diagnostic_sha": diagnostic, "runtime_sha": PRODUCT, "sdk_sha": SDK,
              "product_and_sdk_verified": True, "runtime_files": runtime_files, "tool_files": tools,
              "sdk_files": {name: checksum(sdk / name) for name in sorted(sdk_files)},
              "harness_sha256": harness_digest, "workflow_sha256": tools[WORKFLOW],
              "all_working_bytes_match_head": True, "actual_filesystem_and_tracked_inventory_exact": True,
              "no_symlinks_or_untracked_or_ignored_inputs": True,
              "runtime_go_files": sum(n.endswith(".go") and not n.endswith("_test.go") for n in base_files),
              "sdk_runtime_go_files": sum(n.endswith(".go") and not n.endswith("_test.go") for n in sdk_files),
              "additional_tagged_getter_sha256": tools[GETTER], "database_access": False}
    assert result["runtime_go_files"] == 89 and result["sdk_runtime_go_files"] == 15
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    target = output / ("source-map-" + phase + ".json")
    with target.open("x") as stream:
        stream.write(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--sdk", type=Path, required=True)
    parser.add_argument("--phase", choices=("before", "after"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        verify(args.source, args.sdk, args.phase, args.output)
    except BaseException:
        print("Current-candidate finite source verification failed.", file=sys.stderr)
        sys.exit(1)
    print("Current-candidate finite source verification passed.")
