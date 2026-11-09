#!/usr/bin/env python3
"""Finite infrastructure admission only; no DB or credential access."""
import argparse
import json
import math
from pathlib import Path
import time

JOB_SECONDS = 20 * 60
REQUIRED_SECONDS = 11 * 60


def sufficient(start, now):
    elapsed = now - start
    return (math.isfinite(start) and math.isfinite(now) and 0 <= elapsed <= JOB_SECONDS
            and JOB_SECONDS - elapsed >= REQUIRED_SECONDS)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--job-start", type=float, required=True)
    parser.add_argument("--public", type=Path, required=True)
    args = parser.parse_args()
    now = time.monotonic()
    allowed = sufficient(args.job_start, now)
    args.public.mkdir(mode=0o700, parents=True, exist_ok=True)
    receipt = {"infrastructure_job_seconds": JOB_SECONDS, "required_remaining_seconds": REQUIRED_SECONDS,
               "diagnostic_outer_seconds": 420, "kill_after_seconds": 10, "finalization_reserved_seconds": 230,
               "go_application_budgets_unchanged": True, "owner_spawn_admitted": allowed,
               "database_requests": 0}
    with (args.public / "infra-budget.json").open("x") as stream:
        stream.write(json.dumps(receipt, indent=2) + "\n")
    if not allowed:
        with (args.public / "process.json").open("x") as stream:
            stream.write(json.dumps({"diagnostic_launch_attempted": False, "launch_refused_before_owner": True,
                                     "process_wait_returned": False, "process_exit_code": None,
                                     "watchdog_terminated": False}) + "\n")
        print("Finite owner refused before launch: insufficient infrastructure time.")
        return 1
    print("Finite owner admitted within unchanged application/process budgets.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
