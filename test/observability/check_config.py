#!/usr/bin/env python3
"""Validate shipped monitoring configuration with the pinned Prometheus image."""
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]
STACK = ROOT / "ops" / "observability"


def compose(*args, body=None):
    return subprocess.run(["docker", "compose", "--progress", "quiet", *args], cwd=STACK, input=body,
                          text=True, capture_output=True, timeout=120)


def promtool(*args, body=None):
    return compose("run", "--rm", "-T", "--no-deps", "--entrypoint", "/bin/promtool",
                   "prometheus", *args, body=body)


def passed(result, label):
    if result.returncode:
        raise SystemExit(label + " failed:\n" + result.stdout + result.stderr)
    print("PASS " + label, flush=True)


def main():
    passed(compose("config", "--quiet"), "Compose configuration")
    passed(promtool("check", "config", "/etc/prometheus/prometheus.yml"),
           "Prometheus configuration and all operating/demo rules")
    board = json.loads((STACK / "grafana/dashboards/mqlite.json").read_text())
    if board.get("uid") != "mqlite-observability" or not board.get("panels"):
        raise SystemExit("Grafana dashboard has no identity or panels")
    print("PASS Grafana dashboard JSON", flush=True)

    golden = (ROOT / "server/testdata/observability.prom").read_text()
    passed(promtool("check", "metrics", body=golden),
           "full exposition contract and metric naming")

    result = promtool("check", "metrics", body='# TYPE broken gauge\nbroken{label="unfinished} 1\n')
    if result.returncode == 0 or "parsing error" not in result.stdout + result.stderr:
        raise SystemExit("Malformed exposition negative control was not rejected")
    print("PASS real promtool rejects malformed exposition", flush=True)


if __name__ == "__main__":
    main()
