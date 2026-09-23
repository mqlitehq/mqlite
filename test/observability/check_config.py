#!/usr/bin/env python3
"""Validate shipped monitoring configuration with the pinned Prometheus image."""
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[2]
STACK = ROOT / "ops" / "observability"
LEGACY_WARNINGS = {
    "mqlite_queue_oldest_message_age_ms metric names should not contain abbreviated units",
    'mqlite_queue_total non-counter metrics should not have "_total" suffix',
}


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

    # The v0.3.1 compatibility gauges intentionally keep their original names.
    # Accept exactly those two lints, never an arbitrary non-zero tool result.
    golden = (ROOT / "server/testdata/observability.prom").read_text()
    result = promtool("check", "metrics", body=golden)
    warnings = {line.strip() for line in (result.stdout + result.stderr).splitlines() if line.strip()}
    if result.returncode != 3 or warnings != LEGACY_WARNINGS:
        raise SystemExit("Exposition contract failed promtool validation:\n"
                         + result.stdout + result.stderr)
    print("PASS full exposition contract; only the two known compatibility-name lints", flush=True)

    result = promtool("check", "metrics", body='# TYPE broken gauge\nbroken{label="unfinished} 1\n')
    if result.returncode == 0 or "parsing error" not in result.stdout + result.stderr:
        raise SystemExit("Malformed exposition negative control was not rejected")
    print("PASS real promtool rejects malformed exposition", flush=True)


if __name__ == "__main__":
    main()
