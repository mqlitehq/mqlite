#!/usr/bin/env python3
"""Create local, private credentials and data directories without printing secrets."""
import os
from pathlib import Path
import secrets

ROOT = Path(__file__).resolve().parent


def main():
    os.umask(0o077)
    env = ROOT / ".env"
    previous = {}
    if env.exists():
        previous = dict(line.split("=", 1) for line in env.read_text().splitlines()
                        if line and not line.startswith("#") and "=" in line)
    state = Path(os.environ.get("XDG_STATE_HOME", str(Path.home() / ".local/state")))
    configured = previous.get("OBS_DATA_DIR", "").strip('"')
    requested = os.environ.get("OBS_DATA_DIR", "")
    if configured and requested and Path(configured).resolve() != Path(requested).resolve():
        raise SystemExit("Existing .env selects another data directory; review it before changing the stack.")
    data = Path(requested or configured or str(state / "mqlite-observability-032")).resolve()
    repo = ROOT.parent.parent
    if data == repo or repo in data.parents:
        raise SystemExit("OBS_DATA_DIR must be outside the source checkout.")
    # Docker Compose dotenv supports quoted values; reject interpolation/control characters.
    if any(c in str(data) for c in ("\n", "\r", '"', "\\", "$")):
        raise SystemExit("Use a data directory without line separators, quotes, backslashes or dollar signs.")
    for name in ("secrets", "mqlite", "prometheus", "grafana"):
        directory = data / name
        directory.mkdir(parents=True, exist_ok=True)
        directory.chmod(0o700)
    for name in ("admin", "monitor", "grafana-admin"):
        path = data / "secrets" / (name + ".token")
        if not path.exists():
            # Exclusive creation preserves existing credentials on repeat runs.
            with path.open("x") as stream:
                prefix = "mqk_" if name in ("admin", "monitor") else ""
                stream.write(prefix + secrets.token_hex(32) + "\n")
        path.chmod(0o600)
    if not env.exists():
        env.write_text(f'OBS_UID={os.getuid()}\nOBS_GID={os.getgid()}\nOBS_DATA_DIR="{data}"\n')
    env.chmod(0o600)
    print("Ready: private credentials and persistent data are outside the source checkout.")
    print("Credentials were preserved if this stack was initialized before.")
    print("Next: docker compose up --build -d")


if __name__ == "__main__":
    main()
