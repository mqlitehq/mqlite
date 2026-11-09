#!/usr/bin/env python3
"""Safe before/after evidence for the three immutable verification checkouts."""
import argparse
import json
import os
from pathlib import Path
import re
import subprocess


def inspect(path, expected):
    result = {"expected": expected if re.fullmatch(r"[0-9a-f]{40}", expected) else "invalid", "available": False, "head": "unavailable", "clean": False}
    try:
        head = subprocess.run(["git", "rev-parse", "HEAD"], cwd=path, text=True, capture_output=True)
        status = subprocess.run(["git", "status", "--porcelain"], cwd=path, text=True, capture_output=True)
        actual = head.stdout.strip()
        result.update(available=head.returncode == 0 and status.returncode == 0,
                      head=actual if re.fullmatch(r"[0-9a-f]{40}", actual) else "unavailable",
                      clean=status.returncode == 0 and not status.stdout)
    except OSError:
        pass
    result["passed"] = result["available"] and result["clean"] and result["head"] == expected
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--runtime", required=True)
    parser.add_argument("--sdk", required=True)
    parser.add_argument("--runner", required=True)
    parser.add_argument("--receipt", type=Path, required=True)
    args = parser.parse_args()
    data = {"runtime": inspect(args.runtime, os.environ.get("EXPECTED_RUNTIME", "")),
            "sdk": inspect(args.sdk, os.environ.get("EXPECTED_SDK", "")),
            "runner": inspect(args.runner, os.environ.get("GITHUB_SHA", ""))}
    data["passed"] = all(v["passed"] for v in data.values())
    args.receipt.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    with args.receipt.open("x") as stream:
        stream.write(json.dumps(data, indent=2) + "\n")
    return 0 if data["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
