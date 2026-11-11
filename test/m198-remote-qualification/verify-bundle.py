#!/usr/bin/env python3
"""Check the source-frozen diagnostic bundle before and after execution."""
import hashlib
import json
import os
from pathlib import Path

root = Path(__file__).resolve().parent
manifest = root / "bundle.expected.json"
assert hashlib.sha256(manifest.read_bytes()).hexdigest() == os.environ["M153_BUNDLE_SHA256"]
expected = json.loads(manifest.read_text())
actual = {}
for path in root.rglob("*"):
    assert not path.is_symlink()
    if path.is_file() and path != manifest:
        actual[path.relative_to(root).as_posix()] = hashlib.sha256(path.read_bytes()).hexdigest()
assert actual == expected
assert (os.environ["GITHUB_REPOSITORY"], os.environ["GITHUB_EVENT_NAME"]) == ("mqlitehq/mqlite", "push")
assert os.environ["GITHUB_REF"] in ("refs/heads/diagnostic/qualify-m198-turso",)
print("Frozen diagnostic bundle verified.")
