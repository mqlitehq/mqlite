"""Offline fixtures for every release decision; no tags or artifacts are created."""

import copy
import hashlib
import importlib.util
from pathlib import Path
import re
import unittest


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("release_guard", ROOT / ".github/scripts/release_guard.py")
guard = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(guard)
SHA = "a" * 40
OTHER = "b" * 40
OBJECT = "c" * 40
REPO = "mqlitehq/mqlite"


class Fixture:
    def __init__(self):
        self.env = {"GITHUB_REPOSITORY": REPO, "GITHUB_EVENT_NAME": "push",
                    "GITHUB_REF": "refs/tags/v0.3.0", "GITHUB_SHA": SHA}
        self.head = SHA
        self.event_ref_sha = SHA
        self.local_tag = SHA
        self.version_source = 'package version\nconst Version = "0.3.0"\n'
        self.workflow = {"id": 10, "path": guard.CI_PATH, "name": "CI", "state": "active"}
        self.run = {"id": 20, "workflow_id": 10, "head_sha": SHA, "path": guard.CI_PATH,
                    "name": "CI", "repository": {"full_name": REPO}, "head_repository": {"full_name": REPO},
                    "event": "push", "status": "completed", "conclusion": "success", "run_attempt": 1}
        self.runs = [self.run]
        self.jobs = [{"id": 100 + i, "name": name, "head_sha": SHA, "run_id": 20,
                      "run_attempt": 1, "status": "completed", "conclusion": "success"}
                     for i, name in enumerate(sorted(guard.EXPECTED_JOBS))]
        self.remote = {"type": "commit", "sha": SHA}
        self.tag_objects = {}
        self.final_run = None
        self.final_runs = None
        self.final_remote = None
        self.detail_reads = 0
        self.tag_reads = 0
        self.run_list_reads = 0
        self.page_size = 100
        self.calls = []

    def api(self, path, params=None):
        self.calls.append((path, params))
        if path.startswith("/git/ref/tags/"):
            self.tag_reads += 1
            obj = self.final_remote if self.tag_reads > 1 and self.final_remote else self.remote
            return {"ref": "refs/tags/" + path.split("/")[-1], "object": copy.deepcopy(obj)}
        if path.startswith("/git/tags/"):
            return copy.deepcopy(self.tag_objects[path.split("/")[-1]])
        if path == "/actions/workflows/ci.yml":
            return copy.deepcopy(self.workflow)
        if path == "/actions/workflows/ci.yml/runs":
            if params.get("head_sha") != SHA:
                raise AssertionError("CI must be queried by the tag commit")
            if params["page"] == 1:
                self.run_list_reads += 1
            items = self.final_runs if self.run_list_reads > 1 and self.final_runs else self.runs
            key = "workflow_runs"
        elif path == "/actions/runs/20":
            self.detail_reads += 1
            return copy.deepcopy(self.final_run if self.detail_reads > 1 and self.final_run else self.run)
        elif path == "/actions/runs/20/attempts/" + str(self.run["run_attempt"]) + "/jobs":
            items, key = self.jobs, "jobs"
        else:
            raise AssertionError("unexpected API call " + path)
        if params["per_page"] != 100:
            raise AssertionError("page size must be explicit")
        start = (params["page"] - 1) * self.page_size
        return {"total_count": len(items), key: copy.deepcopy(items[start:start + self.page_size])}

    def git(self, *args):
        if args == ("rev-parse", "HEAD"):
            return self.head
        if args[:3] == ("rev-parse", "--verify", "--end-of-options"):
            return self.event_ref_sha if args[3] == self.env["GITHUB_REF"] + "^{commit}" else self.local_tag
        if args == ("show", SHA + ":internal/version/version.go"):
            return self.version_source
        raise AssertionError("unexpected git call " + repr(args))

    def verify(self):
        return guard.verify(self.env, self.api, self.git)


class ReleaseGuardTests(unittest.TestCase):
    def rejected(self, fixture, pattern=None):
        with self.assertRaisesRegex(guard.GuardError, pattern or ".+"):
            fixture.verify()

    def test_stable_and_rc_outputs(self):
        for version in ("0.3.0", "0.3.0-rc.1", "12.34.56", "12.34.56-rc.99"):
            for event in ("push", "workflow_dispatch"):
                for prefix in (("", "v") if event == "workflow_dispatch" else ("",)):
                    with self.subTest(version=version, event=event, prefix=prefix):
                        f = Fixture()
                        f.env["GITHUB_REF"] = "refs/tags/v" + version
                        f.version_source = 'const Version = "' + version.split("-")[0] + '"'
                        if event == "workflow_dispatch":
                            f.env.update(GITHUB_EVENT_NAME=event, GITHUB_REF="refs/heads/main",
                                         GITHUB_SHA=OTHER, RELEASE_VERSION=prefix + version)
                            f.head = f.event_ref_sha = OTHER
                        result = f.verify()
                        self.assertEqual(result["tag"], "v" + version)
                        self.assertEqual(result["version"], version)
                        self.assertEqual(result["base_version"], version.split("-")[0])
                        self.assertEqual(result["sha"], SHA)
                        self.assertEqual(result["ci_run_id"], "20")
                        expected = ["ghcr.io/" + REPO + ":" + version]
                        if "-" not in version:
                            expected += ["ghcr.io/" + REPO + ":" + ".".join(version.split(".")[:2]),
                                         "ghcr.io/" + REPO + ":latest"]
                        self.assertEqual(result["image_tags"].split(","), expected)
                        self.assertEqual(set(result), {"tag", "version", "base_version", "sha", "image_tags", "ci_run_id"})

    def test_version_grammar_whole_surface(self):
        invalid = ("", "0.3", "0.3.0.1", "01.3.0", "0.03.0", "0.3.00", "-1.3.0", "0.3.0-rc",
                   "0.3.0-rc.0", "0.3.0-rc.01", "0.3.0-rc.-1", "0.3.0-beta.1", "0.3.0-RC.1",
                   "0.3.0+build", "0.3.0-rc.1+build", "0.3.0\n", " 0.3.0", "0.3.0 ", "v0.3.0",
                   "$(whoami)", "0.3.0,latest", "0.3.0/../../main")
        for version in invalid:
            for event in ("push", "workflow_dispatch"):
                with self.subTest(version=version, event=event):
                    f = Fixture()
                    if event == "push":
                        f.env["GITHUB_REF"] = "refs/tags/v" + version
                    else:
                        # A single v prefix is explicitly accepted for manual inputs.
                        if version == "v0.3.0":
                            continue
                        f.env.update(GITHUB_EVENT_NAME=event, RELEASE_VERSION=version)
                    self.rejected(f)

    def test_event_ref_sha_and_repository(self):
        cases = [("GITHUB_EVENT_NAME", v) for v in (None, "pull_request", "schedule", "release", "workflow_run")]
        cases += [("GITHUB_REPOSITORY", v) for v in ("fork/mqlite", "mqlitehq/other", "")]
        cases += [("GITHUB_REF", v) for v in ("refs/heads/main", "v0.3.0", "refs/tags/0.3.0", "")]
        cases += [("GITHUB_SHA", v) for v in (None, "", "a" * 39, "a" * 41, "A" * 40, "g" * 40, OTHER)]
        cases += [("RELEASE_VERSION", "0.3.0")]
        for key, value in cases:
            with self.subTest(key=key, value=value):
                f = Fixture()
                f.env[key] = value
                self.rejected(f)
        for attribute in ("head", "event_ref_sha", "local_tag"):
            with self.subTest(attribute=attribute):
                f = Fixture()
                setattr(f, attribute, OTHER)
                # For push, the event ref and local tag are the same git lookup.
                if attribute == "local_tag":
                    f.env.update(GITHUB_EVENT_NAME="workflow_dispatch", GITHUB_REF="refs/heads/main", RELEASE_VERSION="0.3.0")
                self.rejected(f)
        for ref in ("main", "refs/pull/1/merge", ""):
            f = Fixture()
            f.env.update(GITHUB_EVENT_NAME="workflow_dispatch", GITHUB_REF=ref, RELEASE_VERSION="0.3.0")
            self.rejected(f)

    def test_target_version_constant(self):
        for source in ('const Version = "0.2.0"', 'const Version = "0.3.0-rc.1"',
                       '// const Version = "0.3.0"', 'const Other = "0.3.0"',
                       'const Version = "0.3.0"\nconst Version = "0.3.0"'):
            f = Fixture()
            f.version_source = source
            self.rejected(f, "source constant")

    def test_lightweight_and_annotated_tags(self):
        f = Fixture()
        f.remote = {"type": "tag", "sha": OBJECT}
        f.tag_objects[OBJECT] = {"sha": OBJECT, "object": {"type": "commit", "sha": SHA}}
        self.assertEqual(f.verify()["sha"], SHA)
        for obj in ({"type": "commit", "sha": OTHER}, {"type": "tree", "sha": SHA},
                    {"type": "commit", "sha": "bad"}, {}):
            f = Fixture()
            f.remote = obj
            self.rejected(f)
        f = Fixture()
        f.remote = {"type": "tag", "sha": OBJECT}
        f.tag_objects[OBJECT] = {"sha": OBJECT, "object": f.remote}
        self.rejected(f, "cyclic")
        f.tag_objects[OBJECT] = {"sha": OTHER, "object": {"type": "commit", "sha": SHA}}
        self.rejected(f, "object mismatch")
        f = Fixture()
        f.final_remote = {"type": "commit", "sha": OTHER}
        self.rejected(f, "moved")
        with self.assertRaisesRegex(guard.GuardError, "ref mismatch"):
            guard.remote_tag(lambda *args: {"ref": "refs/tags/v0.2.0", "object": {"type": "commit", "sha": SHA}}, "v0.3.0")
        f = Fixture()
        objects = [format(i, "040x") for i in range(1, 12)]
        f.remote = {"type": "tag", "sha": objects[0]}
        for current, following in zip(objects, objects[1:]):
            f.tag_objects[current] = {"sha": current, "object": {"type": "tag", "sha": following}}
        self.rejected(f, "nesting")

    def test_ci_workflow_identity(self):
        for key, values in {"id": (None, 0, True), "path": (None, "ci.yml"),
                            "name": (None, "Other"), "state": (None, "disabled_manually")}.items():
            for value in values:
                f = Fixture()
                f.workflow[key] = value
                self.rejected(f, "workflow identity")

    def test_run_identity_whole_surface(self):
        cases = {"id": (None, 0, True), "head_sha": (None, OTHER), "workflow_id": (None, 11),
                 "path": (None, "other.yml", ""), "name": (None, "Other"),
                 "repository": (None, {}, {"full_name": "fork/mqlite"}),
                 "head_repository": (None, {}, {"full_name": "fork/mqlite"}),
                 "event": (None, "pull_request", "schedule", "workflow_run", "release")}
        for key, values in cases.items():
            for value in values:
                with self.subTest(key=key, value=value):
                    f = Fixture()
                    f.run[key] = value
                    self.rejected(f, "no push/dispatch CI")
        for event in ("push", "workflow_dispatch"):
            f = Fixture()
            f.run["event"] = event
            f.run["path"] += "@refs/heads/main"
            self.assertEqual(f.verify()["sha"], SHA)

    def test_latest_run_and_completion(self):
        for status in (None, "queued", "in_progress", "waiting", "pending", "requested"):
            f = Fixture()
            f.run["status"] = status
            old = copy.deepcopy(f.run)
            old.update(id=19, status="completed", conclusion="success")
            f.runs.insert(0, old)
            self.rejected(f, "latest eligible")
        for conclusion in (None, "failure", "cancelled", "skipped", "neutral", "timed_out", "action_required", "stale", "startup_failure"):
            f = Fixture()
            f.run["conclusion"] = conclusion
            self.rejected(f, "latest eligible")
        for attempt in (None, 0, True, "1"):
            f = Fixture()
            f.run["run_attempt"] = attempt
            self.rejected(f, "attempt")
        f = Fixture()
        f.runs.append(copy.deepcopy(f.run))
        self.rejected(f, "duplicate workflow")
        f = Fixture()
        f.runs = [copy.deepcopy(f.run)]
        f.run["head_sha"] = OTHER
        self.rejected(f, "identity changed")

    def test_every_required_job_must_succeed_in_same_attempt(self):
        mutations = {"status": (None, "queued", "in_progress", "waiting"),
                     "conclusion": (None, "failure", "cancelled", "skipped", "neutral", "timed_out", "action_required", "stale", "startup_failure"),
                     "head_sha": (None, OTHER), "run_id": (None, 21), "run_attempt": (None, True, 2), "id": (None, 0, True)}
        for index, name in enumerate(sorted(guard.EXPECTED_JOBS)):
            for key, values in mutations.items():
                for value in values:
                    with self.subTest(job=name, key=key, value=value):
                        f = Fixture()
                        f.jobs[index][key] = value
                        self.rejected(f)
            for operation in ("missing", "duplicate", "renamed"):
                with self.subTest(job=name, operation=operation):
                    f = Fixture()
                    if operation == "missing":
                        f.jobs.pop(index)
                    elif operation == "duplicate":
                        f.jobs.append(copy.deepcopy(f.jobs[index]))
                    else:
                        f.jobs[index]["name"] += " changed"
                    self.rejected(f, "job set")
        f = Fixture()
        f.jobs[0]["id"] = f.jobs[1]["id"]
        self.rejected(f, "duplicate CI job")

    def test_attempt_specific_jobs_and_rerun_race(self):
        f = Fixture()
        f.run["run_attempt"] = 2
        for job in f.jobs:
            job["run_attempt"] = 2
        self.assertEqual(f.verify()["sha"], SHA)
        self.assertTrue(any(path.endswith("/attempts/2/jobs") for path, _ in f.calls))
        f.jobs = f.jobs[:1]
        self.rejected(f, "job set")
        for key, value in (("run_attempt", 2), ("status", "in_progress"), ("conclusion", "failure"),
                           ("id", 21), ("head_sha", OTHER), ("event", "pull_request")):
            f = Fixture()
            f.final_run = copy.deepcopy(f.run)
            f.final_run[key] = value
            self.rejected(f, "changed during")
        for status, conclusion in (("in_progress", None), ("completed", "failure"), ("completed", "success")):
            f = Fixture()
            f.final_runs = [f.run, dict(copy.deepcopy(f.run), id=21, status=status, conclusion=conclusion)]
            self.rejected(f, "newer CI run")

    def test_all_pages_in_runs_and_jobs(self):
        f = Fixture()
        f.page_size = 3
        f.runs = [dict(copy.deepcopy(f.run), id=i, head_sha=OTHER) for i in range(1, 8)] + [f.run]
        self.assertEqual(f.verify()["sha"], SHA)
        self.assertTrue(any(path.endswith("/runs") and params["page"] == 3 for path, params in f.calls))
        self.assertTrue(any(path.endswith("/jobs") and params["page"] == 4 for path, params in f.calls))
        for responses in ([{"total_count": 1, "jobs": []}], [{"total_count": 0, "jobs": [{}]}],
                          [{"total_count": -1, "jobs": []}], [{"total_count": True, "jobs": []}],
                          [{"total_count": 1, "jobs": None}],
                          [{"total_count": 2, "jobs": [{}]}, {"total_count": 3, "jobs": [{}]}]):
            with self.subTest(responses=responses):
                iterator = iter(responses)
                with self.assertRaises(guard.GuardError):
                    guard.pages(lambda *args: next(iterator), "/jobs", "jobs")
        self.assertEqual(guard.pages(lambda *args: {"total_count": 0, "jobs": []}, "/jobs", "jobs"), [])
        with self.assertRaisesRegex(guard.GuardError, "exceeded"):
            guard.pages(lambda *args: {"total_count": 101, "jobs": [{}]}, "/jobs", "jobs")


class WorkflowContractTests(unittest.TestCase):
    def test_ci_job_set_is_explicit_and_complete(self):
        ci = (ROOT / guard.CI_PATH).read_text()
        self.assertEqual(set(re.findall(r"^  ([a-z0-9_]+):$", ci.split("jobs:\n", 1)[1], re.MULTILINE)),
                         {"test", "coverage", "lint", "govulncheck", "docker", "e2e", "crash"})
        names = set(re.findall(r"^    name: (.+)$", ci, re.MULTILINE))
        names.remove("test (${{ matrix.os }} · go ${{ matrix.go }})")
        names.update("test (" + os + " · go " + go + ")" for os, go in (
            ("ubuntu-latest", "1.21.x"), ("ubuntu-latest", "stable"), ("macos-14", "1.21.x"),
            ("macos-latest", "stable"), ("windows-latest", "1.21.x"), ("windows-latest", "stable")))
        self.assertEqual(names, guard.EXPECTED_JOBS)
        self.assertEqual(len(names), 12)

    def test_workflow_surface_requires_deliberate_review(self):
        # Hash the complete workflows, not selected "good" lines: an added if,
        # continue-on-error, alternate publish step, matrix entry, or checkout is
        # security-relevant. Review any change before updating these goldens.
        expected = {
            "ci.yml": "5f750ec6738e3086d89ab4971020394a1f28d3018f220738a7fe8e351c0cab69",
            "release.yml": "badec32ab2c9e80b37e732ae0e4418ceebabb3de80b4a396d87dd0c799ab4ed4",
            "release-image.yml": "00ce4b311619e3f573537516713741efc523d0fb2b1692cacb81d8e5e5219643",
        }
        self.assertEqual({p.name for p in (ROOT / ".github/workflows").iterdir()},
                         {"ci.yml", "release.yml", "release-image.yml", "turso-nightly.yml", "integrity-weekly.yml"})
        for name, digest in expected.items():
            with self.subTest(workflow=name):
                self.assertEqual(hashlib.sha256((ROOT / ".github/workflows" / name).read_bytes()).hexdigest(), digest,
                                 "Review the entire release/CI contract, then update its golden")
        self.assertEqual(hashlib.sha256((ROOT / "Dockerfile").read_bytes()).hexdigest(), "80390edb5646664e9dead97bb0b1dac0e91835e81066b858182c7cecdef3a497",
                         "Review runtime APK upgrades, stage names, and artifact scans before updating the golden")
        self.assertEqual(hashlib.sha256((ROOT / ".goreleaser.yaml").read_bytes()).hexdigest(), "57a16550597eab93319f89e89f93299ca3853fbccdb4873b922335ff952dc67a",
                         "Review every release build target, flag, and binary scan hook before updating the golden")

    def test_image_verification_command_contract(self):
        # The scanner flags and promotion commands moved out of YAML; pin that
        # entire execution surface too, so bypasses require deliberate review.
        self.assertEqual(hashlib.sha256((ROOT / ".github/scripts/image_candidate.py").read_bytes()).hexdigest(),
                         "fbd87cbd5648aafa5f075b43d4620304b027d87f92c312093818c35f2bebb34d", "Review the complete image gate before updating its golden")

    def test_shared_guard_is_the_only_publish_gate(self):
        for name in ("release.yml", "release-image.yml"):
            text = (ROOT / ".github/workflows" / name).read_text()
            self.assertEqual(text.count("run: python3 .github/scripts/release_guard.py"), 1)
            self.assertIn("actions: read", text)
            self.assertIn("ref: ${{ steps.release.outputs.sha }}", text)
            self.assertLess(text.index("release_guard.py"), text.index("ref: ${{ steps.release.outputs.sha }}"))
            self.assertLess(text.index("verify build checkout"), text.index("- uses: actions/setup-go") if name == "release.yml"
                            else text.index("- uses: docker/setup-buildx"))
        docker = (ROOT / "Dockerfile").read_text()
        self.assertIn("-mode=binary /out/mqlite", docker)
        self.assertIn('-ldflags "-w"', docker)
        self.assertNotIn('-ldflags "-s', docker)
        self.assertNotIn("-json", docker)
        self.assertLess(docker.index("-mode=binary /out/mqlite"), docker.index("COPY --from=build /out/mqlite"))
        self.assertIn("org.opencontainers.image.version=$VERSION", docker)
        self.assertIn("org.opencontainers.image.revision=$REVISION", docker)
        release = (ROOT / ".goreleaser.yaml").read_text()
        builds = release.split("builds:\n", 1)[1].split("\narchives:", 1)[0]
        self.assertEqual(re.findall(r"^  - id: (.+)$", builds, re.MULTILINE), ["mqlite", "mqlite-mcp"])
        for build in builds.split("  - id: ")[1:]:
            self.assertIn("ldflags: [-w]", build)
            self.assertIn("goos: [linux, darwin, windows]", build)
            self.assertIn("goarch: [amd64, arm64]", build)
            self.assertIn('post:\n        - cmd: go run golang.org/x/vuln/cmd/govulncheck@latest -mode=binary "{{ .Path }}"', build)
            self.assertNotIn("-json", build)
        self.assertIn("args: build --snapshot --clean", (ROOT / guard.CI_PATH).read_text())


if __name__ == "__main__":
    unittest.main()
