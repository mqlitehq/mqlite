#!/usr/bin/env python3
"""Fail closed before publishing: a real tag, matching source, and complete CI."""

import collections
import json
import os
import re
import subprocess
import sys
import urllib.parse
import urllib.request


CI_PATH = ".github/workflows/ci.yml"
EXPECTED_JOBS = frozenset({
    "test (ubuntu-latest · go 1.21.x)",
    "test (ubuntu-latest · go stable)",
    "test (macos-14 · go 1.21.x)",
    "test (macos-latest · go stable)",
    "test (windows-latest · go 1.21.x)",
    "test (windows-latest · go stable)",
    "coverage (per-package gate, cross-package)",
    "golangci-lint",
    "govulncheck",
    "docker build + authenticated restart smoke",
    "e2e (curl + python + SDK blackbox)",
    "crash injection (recovery invariants)",
})
VERSION_RE = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(?:-rc\.([1-9][0-9]*))?")
SHA_RE = re.compile(r"[0-9a-f]{40}")


class GuardError(Exception):
    pass


def require(condition, message):
    if not condition:
        raise GuardError(message)


def sha(value):
    require(isinstance(value, str) and SHA_RE.fullmatch(value), "invalid commit/object SHA")
    return value


def positive_int(value):
    return type(value) is int and value > 0


class GitHub:
    def __init__(self, repository, token):
        self.base = "https://api.github.com/repos/" + repository
        self.token = token

    def __call__(self, path, params=None):
        url = self.base + path
        if params:
            url += "?" + urllib.parse.urlencode(params)
        request = urllib.request.Request(url, headers={
            "Authorization": "Bearer " + self.token,
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
            "User-Agent": "mqlite-release-guard",
        })
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.load(response)


def pages(api, path, key, params=None):
    """Read every page; a changing/incomplete collection requires a fresh check."""
    result = []
    total = None
    for page in range(1, 101):
        data = api(path, dict(params or {}, per_page=100, page=page))
        count = data.get("total_count")
        items = data.get(key)
        require(type(count) is int and count >= 0 and isinstance(items, list), "invalid API pagination")
        if total is None:
            total = count
        require(count == total, "API collection changed during pagination")
        result.extend(items)
        require(len(result) <= total, "API returned more entries than total_count")
        if len(result) == total:
            return result
        require(items, "API pagination ended before total_count")
    raise GuardError("API pagination exceeded 100 pages")


def remote_tag(api, tag):
    ref = "refs/tags/" + tag
    data = api("/git/ref/tags/" + tag)
    require(data.get("ref") == ref, "remote tag ref mismatch")
    obj = data.get("object", {})
    seen = set()
    for _ in range(10):
        value = sha(obj.get("sha"))
        require(value not in seen, "cyclic annotated tag")
        seen.add(value)
        if obj.get("type") == "commit":
            return value
        require(obj.get("type") == "tag", "tag does not resolve to a commit")
        data = api("/git/tags/" + value)
        require(data.get("sha") == value, "annotated tag object mismatch")
        obj = data.get("object", {})
    raise GuardError("annotated tag nesting exceeds 10 objects")


def run_identity(run, repository, target, workflow_id):
    return (
        run.get("head_sha") == target
        and run.get("workflow_id") == workflow_id
        and (run.get("path") or "").split("@", 1)[0] == CI_PATH
        and run.get("name") == "CI"
        and (run.get("repository") or {}).get("full_name") == repository
        and (run.get("head_repository") or {}).get("full_name") == repository
        and run.get("event") in ("push", "workflow_dispatch")
        and positive_int(run.get("id"))
    )


def latest_run(api, repository, target, workflow_id):
    runs = pages(api, "/actions/workflows/ci.yml/runs", "workflow_runs", {"head_sha": target})
    require(len({run.get("id") for run in runs}) == len(runs), "duplicate workflow runs")
    eligible = [run for run in runs if run_identity(run, repository, target, workflow_id)]
    require(eligible, "no push/dispatch CI run for the release commit")
    # A new pending/failed run must not be hidden by an older successful run.
    return max(eligible, key=lambda run: run["id"])


def check_ci(api, repository, target):
    workflow = api("/actions/workflows/ci.yml")
    require(workflow.get("path") == CI_PATH and workflow.get("name") == "CI"
            and workflow.get("state") == "active" and positive_int(workflow.get("id")),
            "CI workflow identity mismatch or inactive")
    listed = latest_run(api, repository, target, workflow["id"])
    path = "/actions/runs/" + str(listed["id"])
    run = api(path)
    require(run_identity(run, repository, target, workflow["id"]) and run["id"] == listed["id"],
            "CI run identity changed")
    require(run.get("status") == "completed" and run.get("conclusion") == "success",
            "latest eligible CI run is not completed successfully")
    attempt = run.get("run_attempt")
    require(positive_int(attempt), "invalid CI run attempt")
    # Never combine jobs from different attempts. Rerun all jobs after a failure.
    jobs = pages(api, path + "/attempts/" + str(attempt) + "/jobs", "jobs")
    require(collections.Counter(job.get("name") for job in jobs) == collections.Counter(EXPECTED_JOBS),
            "CI job set differs from the required 12 jobs (missing, extra, or duplicate)")
    require(all(positive_int(job.get("id")) for job in jobs)
            and len({job["id"] for job in jobs}) == len(jobs), "invalid or duplicate CI job IDs")
    for job in jobs:
        require(job.get("head_sha") == target and job.get("run_id") == run["id"]
                and positive_int(job.get("run_attempt")) and job["run_attempt"] == attempt,
                "CI job belongs to a different commit/run/attempt")
        require(job.get("status") == "completed" and job.get("conclusion") == "success",
                "CI job did not complete successfully: " + job["name"])
    require(latest_run(api, repository, target, workflow["id"])["id"] == run["id"],
            "a newer CI run started during verification")
    final = api(path)
    require(run_identity(final, repository, target, workflow["id"])
            and final.get("id") == run["id"] and final.get("run_attempt") == attempt
            and final.get("status") == "completed" and final.get("conclusion") == "success",
            "CI was rerun or changed during verification")
    return run["id"]


def git(*args):
    return subprocess.check_output(["git", *args], text=True).strip()


def verify(env, api, git_command=git):
    repository = env.get("GITHUB_REPOSITORY", "")
    require(repository == "mqlitehq/mqlite", "unexpected release repository")
    event = env.get("GITHUB_EVENT_NAME")
    ref = env.get("GITHUB_REF", "")
    event_sha = sha(env.get("GITHUB_SHA"))
    require(sha(git_command("rev-parse", "HEAD")) == event_sha, "initial checkout differs from event SHA")
    requested = env.get("RELEASE_VERSION", "")
    if event == "push":
        require(not requested and ref.startswith("refs/tags/v"), "push must target a version tag")
        version = ref[len("refs/tags/v"):]
    else:
        require(event == "workflow_dispatch", "only tag pushes or manual dispatch can release")
        require(ref.startswith("refs/heads/") or ref.startswith("refs/tags/"), "invalid dispatch ref")
        require(requested, "manual image release requires an existing version tag")
        version = requested[1:] if requested.startswith("v") else requested
    match = VERSION_RE.fullmatch(version)
    require(match, "expected X.Y.Z or X.Y.Z-rc.N (optional v prefix for manual input)")
    require(sha(git_command("rev-parse", "--verify", "--end-of-options", ref + "^{commit}")) == event_sha,
            "event ref differs from event SHA")
    tag = "v" + version
    target = remote_tag(api, tag)
    local = sha(git_command("rev-parse", "--verify", "--end-of-options", "refs/tags/" + tag + "^{commit}"))
    require(local == target, "local and remote tag commits differ")
    if event == "push":
        require(event_sha == target, "tag push SHA differs from the tag commit")
    source = git_command("show", target + ":internal/version/version.go")
    versions = re.findall(r'^const Version = "([^"]+)"$', source, re.MULTILINE)
    base = ".".join(match.group(i) for i in (1, 2, 3))
    require(versions == [base], "tag base version differs from the tagged source constant")
    run_id = check_ci(api, repository, target)
    # Detect a tag move while CI was being inspected.
    require(remote_tag(api, tag) == target, "remote tag moved during verification")
    image = "ghcr.io/" + repository
    tags = [image + ":" + version]
    if match.group(4) is None:
        tags += [image + ":" + ".".join(match.group(i) for i in (1, 2)), image + ":latest"]
    return {"tag": tag, "version": version, "base_version": base, "sha": target,
            "image_tags": ",".join(tags), "ci_run_id": str(run_id)}


def main():
    try:
        require(os.environ.get("GITHUB_TOKEN"), "GITHUB_TOKEN is required")
        result = verify(os.environ, GitHub(os.environ.get("GITHUB_REPOSITORY", ""), os.environ["GITHUB_TOKEN"]))
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            output.writelines(key + "=" + value + "\n" for key, value in result.items())
        print(json.dumps(result, sort_keys=True))
    except (GuardError, KeyError, ValueError, OSError, subprocess.CalledProcessError) as error:
        print("release guard: " + str(error), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
