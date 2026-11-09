#!/usr/bin/env python3
"""Bind a copied support module to exact checkout identities; never read secrets."""
import os
import pathlib
import re
import subprocess
import sys

if len(sys.argv) != 3:
    raise SystemExit("two reviewed source checkouts are required")
root = pathlib.Path(__file__).resolve().parent
for label, path, variable in [("runtime", sys.argv[1], "EXPECTED_RUNTIME"), ("sdk", sys.argv[2], "EXPECTED_SDK")]:
    expected = os.environ.get(variable, "")
    if not re.fullmatch(r"[0-9a-f]{40}", expected):
        raise SystemExit("immutable source identity required")
    source = pathlib.Path(path).resolve()
    actual = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=source, text=True).strip()
    status = subprocess.check_output(["git", "status", "--porcelain"], cwd=source, text=True)
    if actual != expected or status:
        raise SystemExit("source identity/cleanliness mismatch")
    module = "github.com/mqlitehq/mqlite" + ("-go" if label == "sdk" else "")
    subprocess.run(["go", "mod", "edit", "-replace", f"{module}={source}"], cwd=root, check=True)
