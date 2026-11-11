"""Verify the exact frozen qualification bundle and workflow context."""
import hashlib,json,os
from pathlib import Path
root=Path(__file__).resolve().parent
manifest=root/"bundle.expected.json"
assert hashlib.sha256(manifest.read_bytes()).hexdigest()==os.environ["M194_BUNDLE_SHA256"]
actual={}
for path in root.rglob("*"):
    assert not path.is_symlink()
    if path.is_file() and path!=manifest:
        actual[path.relative_to(root).as_posix()]=hashlib.sha256(path.read_bytes()).hexdigest()
assert actual==json.loads(manifest.read_text())
assert (os.environ["GITHUB_REPOSITORY"],os.environ["GITHUB_EVENT_NAME"],os.environ["GITHUB_REF"])==("mqlitehq/mqlite","push","refs/heads/diagnostic/m194-m198-loopback-linux")
print("Frozen final qualification inputs verified.")
