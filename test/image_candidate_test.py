#!/usr/bin/env python3
"""Negative controls for the actual multi-platform image publishing boundary."""
import copy
from contextlib import contextmanager
from datetime import date, datetime, timedelta
import errno
import http.client
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import socket
import ssl
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
import urllib.error
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("candidate", ROOT / ".github/scripts/image_candidate.py")
candidate = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(candidate)
SMOKE_SPEC = importlib.util.spec_from_file_location("release_smoke", ROOT / "test/release_image_smoke.py")
release_smoke = importlib.util.module_from_spec(SMOKE_SPEC)
SMOKE_SPEC.loader.exec_module(release_smoke)
SHA = "1" * 40
SCAN_DATE = date(2026, 9, 10)


class DownloadTests(unittest.TestCase):
    """No network or real sleeps: exercise the production downloader and its hard timers."""
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.destination = self.root / "trivy.tar.gz"
        self.now = 0
        self.original_open = candidate.open_trivy
        self.previous_handler = object()
        self.download = self.enterContext(patch.object(candidate, "open_trivy"))
        self.download.side_effect = AssertionError("unexpected network request")
        self.enterContext(patch.object(candidate, "TRIVY_SHA256", candidate.digest(b"verified archive")))
        self.enterContext(patch.object(candidate.time, "monotonic", side_effect=lambda: self.now))
        self.sleep = self.enterContext(patch.object(candidate.time, "sleep", side_effect=self.advance))
        self.gettimer = self.enterContext(patch.object(candidate.signal, "getitimer", return_value=(0, 0)))
        self.getsignal = self.enterContext(patch.object(candidate.signal, "getsignal", return_value=self.previous_handler))
        self.settimer = self.enterContext(patch.object(candidate.signal, "setitimer"))
        self.setsignal = self.enterContext(patch.object(candidate.signal, "signal"))
        self.enterContext(patch("builtins.print"))

    def advance(self, seconds):
        self.now += seconds

    def assert_clean(self):
        self.assertEqual(list(self.root.glob(".*.part")), [])
        self.assertEqual(self.settimer.call_args.args, (candidate.signal.ITIMER_REAL, 0))
        self.assertEqual(self.setsignal.call_args.args, (candidate.signal.SIGALRM, self.previous_handler))

    def http_error(self, status):
        return urllib.error.HTTPError(candidate.TRIVY_URL, status, "fixture", {}, io.BytesIO())

    def test_retryable_statuses_retry_the_pinned_download_and_verify_every_byte(self):
        for status in (408, 429, 500, 502, 503, 504):
            with self.subTest(status=status):
                self.download.reset_mock()
                self.sleep.reset_mock()
                self.download.side_effect = [self.http_error(status), io.BytesIO(b"verified archive")]
                candidate.download_trivy(self.destination)
                self.assertEqual(self.download.call_count, 2)
                self.assertEqual(self.sleep.call_args_list, [unittest.mock.call(2)])
                self.assertEqual(self.destination.read_bytes(), b"verified archive")
                self.assert_clean()
                self.destination.unlink()

    def test_three_failures_stop_without_a_partial_or_success_artifact(self):
        self.download.side_effect = [self.http_error(504) for _ in range(3)]
        with self.assertRaisesRegex(RuntimeError, "three transient failures: HTTP 504"):
            candidate.download_trivy(self.destination)
        self.assertEqual(self.download.call_count, 3)
        self.assertEqual(self.sleep.call_args_list, [unittest.mock.call(2), unittest.mock.call(4)])
        self.assertFalse(self.destination.exists())
        self.assert_clean()

    def test_interrupted_body_starts_a_fresh_file_and_does_not_resume_partial_bytes(self):
        class Interrupted(io.BytesIO):
            def read(self, _size):
                if self.tell() == 0:
                    return super().read(4)
                raise http.client.IncompleteRead(b"untrusted partial")
        self.download.side_effect = [Interrupted(b"bad bytes"), io.BytesIO(b"verified archive")]
        candidate.download_trivy(self.destination)
        self.assertEqual(self.destination.read_bytes(), b"verified archive")
        self.assertEqual(self.download.call_count, 2)
        self.assert_clean()

    def test_network_classification_does_not_retry_certificates_or_unknown_failures(self):
        errors = [self.http_error(status) for status in (302, 400, 401, 403, 404, 410, 501, 505)]
        errors += [ssl.SSLCertVerificationError("fixture certificate"), ssl.SSLError("fixture TLS"),
                   socket.gaierror(socket.EAI_NONAME, "fixture DNS"),
                   urllib.error.URLError("unknown reason"), OSError(errno.ENOSPC, "fixture disk")]
        for error in errors:
            with self.subTest(error=type(error).__name__, status=getattr(error, "code", None)):
                self.download.reset_mock()
                self.sleep.reset_mock()
                self.download.side_effect = error
                with self.assertRaises(type(error)):
                    candidate.download_trivy(self.destination)
                self.assertEqual(self.download.call_count, 1)
                self.sleep.assert_not_called()
                self.assertFalse(self.destination.exists())
                self.assert_clean()

    def test_explicit_transport_failures_retry_but_generic_os_errors_do_not(self):
        for error in (TimeoutError(), ConnectionResetError(), ConnectionRefusedError(),
                      urllib.error.URLError(socket.gaierror(socket.EAI_AGAIN, "fixture")),
                      http.client.RemoteDisconnected()):
            with self.subTest(error=type(error).__name__):
                self.download.reset_mock()
                self.download.side_effect = [error, io.BytesIO(b"verified archive")]
                candidate.download_trivy(self.destination)
                self.assertEqual(self.download.call_count, 2)
                self.destination.unlink()
                self.assert_clean()

    def test_hash_local_io_and_publish_failures_never_retry(self):
        cases = ((candidate, "TRIVY_SHA256", "0" * 64),
                 (candidate, "file_digest", unittest.mock.Mock(side_effect=OSError(errno.ENOSPC, "fixture hash"))),
                 (candidate.os, "link", unittest.mock.Mock(side_effect=OSError(errno.ECONNRESET, "fixture publish"))))
        for target, name, value in cases:
            with self.subTest(stage=name):
                self.download.reset_mock()
                self.sleep.reset_mock()
                self.download.side_effect = lambda _timeout: io.BytesIO(b"verified archive")
                with patch.object(target, name, value), self.assertRaises((RuntimeError, OSError)):
                    candidate.download_trivy(self.destination)
                self.assertEqual(self.download.call_count, 1)
                self.sleep.assert_not_called()
                self.assertFalse(self.destination.exists())
                self.assert_clean()
        with patch.object(candidate.os, "open", side_effect=OSError(errno.ENOSPC, "fixture open")):
            self.download.reset_mock()
            with self.assertRaises(OSError):
                candidate.download_trivy(self.destination)
            self.download.assert_not_called()
        self.assert_clean()

    def test_partial_local_write_failure_is_not_a_retryable_network_error(self):
        original_fdopen = candidate.os.fdopen
        class BrokenWriter:
            def __init__(self, stream):
                self.stream = stream
            def __enter__(self):
                return self
            def __exit__(self, *_args):
                self.stream.close()
            def write(self, block):
                self.stream.write(block[:3])
                raise OSError(errno.ENOSPC, "fixture write")
        def open_file(descriptor, mode):
            return BrokenWriter(original_fdopen(descriptor, mode))
        self.download.side_effect = lambda _timeout: io.BytesIO(b"verified archive")
        with patch.object(candidate.os, "fdopen", side_effect=open_file), self.assertRaises(OSError):
            candidate.download_trivy(self.destination)
        self.assertEqual(self.download.call_count, 1)
        self.sleep.assert_not_called()
        self.assertFalse(self.destination.exists())
        self.assert_clean()

    def test_partial_and_published_archive_are_private_even_with_a_permissive_umask(self):
        previous = candidate.os.umask(0o022)
        try:
            def download(_timeout):
                parts = list(self.root.glob(".*.part"))
                self.assertEqual(len(parts), 1)
                self.assertEqual(parts[0].stat().st_mode & 0o777, 0o600)
                return io.BytesIO(b"verified archive")
            self.download.side_effect = download
            candidate.download_trivy(self.destination)
            self.assertEqual(self.destination.stat().st_mode & 0o777, 0o600)
        finally:
            candidate.os.umask(previous)
        self.assert_clean()

    def test_destination_and_unrelated_partial_are_never_overwritten_or_deleted(self):
        self.destination.write_bytes(b"existing")
        with self.assertRaisesRegex(RuntimeError, "already exists"):
            candidate.download_trivy(self.destination)
        self.download.assert_not_called()
        self.assertEqual(self.destination.read_bytes(), b"existing")
        self.destination.unlink()
        unrelated = self.root / ".trivy.tar.gz.someone-else.part"
        unrelated.write_bytes(b"not ours")
        original_link = candidate.os.link
        def concurrent_file(source, destination):
            destination.write_bytes(b"concurrent owner")
            original_link(source, destination)
        self.download.side_effect = lambda _timeout: io.BytesIO(b"verified archive")
        with patch.object(candidate.os, "link", side_effect=concurrent_file), self.assertRaises(FileExistsError):
            candidate.download_trivy(self.destination)
        self.assertEqual(self.destination.read_bytes(), b"concurrent owner")
        self.assertEqual(unrelated.read_bytes(), b"not ours")
        self.assertEqual(list(self.root.glob(".*.part")), [unrelated])

    def test_exclusive_partial_collision_does_not_delete_another_owner_file(self):
        partial = self.root / ".trivy.tar.gz.collision.part"
        partial.write_bytes(b"another owner")
        with patch.object(candidate.uuid, "uuid4", return_value=SimpleNamespace(hex="collision")):
            with self.assertRaises(FileExistsError):
                candidate.download_trivy(self.destination)
        self.download.assert_not_called()
        self.assertEqual(partial.read_bytes(), b"another owner")
        self.assertFalse(self.destination.exists())
        self.assertEqual(list(self.root.glob(".*.part")), [partial])

    def test_hard_attempt_deadline_retries_and_hard_total_deadline_does_not(self):
        class SlowResponse(io.BytesIO):
            def read(response, size):
                self.advance(60)
                self.setsignal.call_args_list[0].args[1](candidate.signal.SIGALRM, None)
        self.download.side_effect = [SlowResponse(b"never complete"), io.BytesIO(b"verified archive")]
        candidate.download_trivy(self.destination)
        self.assertEqual(self.download.call_count, 2)
        self.assertEqual(self.sleep.call_args_list, [unittest.mock.call(2)])
        self.assertTrue(any(call.args == (candidate.signal.ITIMER_REAL, 60)
                            for call in self.settimer.call_args_list))
        self.assert_clean()
        self.destination.unlink()
        self.download.reset_mock()
        # The current handler is the last installation, before the restore call.
        def install(_signal, handler):
            if callable(handler):
                self.handler = handler
        self.setsignal.side_effect = install
        def exhausted(_timeout):
            self.advance(180)
            self.handler(candidate.signal.SIGALRM, None)
        self.download.side_effect = exhausted
        with self.assertRaises(candidate.DownloadBudgetExpired):
            candidate.download_trivy(self.destination)
        self.assertEqual(self.download.call_count, 1)
        self.assertFalse(self.destination.exists())
        self.assert_clean()

    def test_third_attempt_uses_the_remaining_total_budget_not_another_full_minute(self):
        timeouts = []
        def response(timeout):
            timeouts.append(timeout)
            self.advance(60 if len(timeouts) < 3 else 54)
            raise TimeoutError()
        self.download.side_effect = response
        with self.assertRaises(candidate.DownloadBudgetExpired):
            candidate.download_trivy(self.destination)
        self.assertEqual(timeouts, [60, 60, 54])
        self.assertEqual(self.now, 180)
        self.assert_clean()

    def test_attempt_timeout_keeps_a_hard_total_timer_while_response_cleanup_unwinds(self):
        def install(_signal, handler):
            if callable(handler):
                self.handler = handler
        self.setsignal.side_effect = install
        class SlowCleanup:
            def __enter__(response):
                return response
            def read(response, _size):
                self.advance(60)
                self.handler(candidate.signal.SIGALRM, None)
            def __exit__(response, *_args):
                self.assertEqual(self.settimer.call_args.args, (candidate.signal.ITIMER_REAL, 120))
                self.advance(120)
                self.handler(candidate.signal.SIGALRM, None)
        self.download.side_effect = [SlowCleanup()]
        with self.assertRaises(candidate.DownloadBudgetExpired):
            candidate.download_trivy(self.destination)
        self.assertEqual(self.now, 180)
        self.assertEqual(self.download.call_count, 1)
        self.assertFalse(self.destination.exists())
        self.sleep.assert_not_called()
        self.assert_clean()

    def test_no_retry_when_the_remaining_budget_cannot_cover_backoff(self):
        def failure(_timeout):
            self.advance(179)
            raise TimeoutError()
        self.download.side_effect = failure
        with self.assertRaises(candidate.DownloadBudgetExpired):
            candidate.download_trivy(self.destination)
        self.assertEqual(self.download.call_count, 1)
        self.sleep.assert_not_called()
        self.assert_clean()

    def test_active_prior_alarm_is_rejected_without_delaying_or_replacing_it(self):
        for timer in ((1, 0), (0, 2)):
            with self.subTest(timer=timer):
                self.gettimer.return_value = timer
                with self.assertRaisesRegex(RuntimeError, "active deadline timer"):
                    candidate.download_trivy(self.destination)
                self.settimer.assert_not_called()
                self.setsignal.assert_not_called()
                self.getsignal.assert_not_called()
                self.download.assert_not_called()

    def test_no_worker_thread_or_unsupported_platform_fallback(self):
        with patch.object(candidate.threading, "current_thread", return_value=object()):
            with self.assertRaisesRegex(RuntimeError, "main-thread POSIX"):
                candidate.download_trivy(self.destination)
        with patch.object(candidate, "signal", SimpleNamespace()):
            with self.assertRaisesRegex(RuntimeError, "main-thread POSIX"):
                candidate.download_trivy(self.destination)
        self.download.assert_not_called()
        self.settimer.assert_not_called()

    def test_fixed_source_and_redirect_allowlist_are_checked_before_following(self):
        with patch.object(candidate.urllib.request, "build_opener") as opener:
            self.original_open(17)
            opener.return_value.open.assert_called_once_with(candidate.TRIVY_URL, timeout=17)
            self.assertEqual(candidate.TRIVY_URL, "https://github.com/aquasecurity/trivy/releases/download/v0.74.0/"
                             "trivy_0.74.0_Linux-64bit.tar.gz")
        handler = candidate.TrivyRedirectHandler()
        request = candidate.urllib.request.Request(candidate.TRIVY_URL)
        for url in ("http://github.com/file", "https://unknown.example/file", "https://github.com.evil/file",
                    "https://user@github.com/file", "https://github.com:444/file"):
            with self.subTest(url=url), self.assertRaisesRegex(RuntimeError, "unapproved"):
                handler.redirect_request(request, None, 302, "fixture", {}, url)
        for host in candidate.TRIVY_DOWNLOAD_HOSTS:
            redirected = handler.redirect_request(request, None, 302, "fixture", {}, "https://" + host + "/file")
            self.assertEqual(redirected.full_url, "https://" + host + "/file")


def build_info(arch):
    return ("fixture-mqlite: go1.27.1\n\tpath\tgithub.com/mqlitehq/mqlite/cmd/mqlite\n"
            "\tbuild\tGOOS=linux\n\tbuild\tGOARCH=" + arch + "\n\tbuild\tCGO_ENABLED=0\n")


def fixture(path, arches=("amd64", "arm64"), mutate=None):
    files = {}

    def blob(value, media_type):
        raw = value if isinstance(value, bytes) else json.dumps(value).encode()
        digest = candidate.digest(raw)
        files["blobs/sha256/" + digest] = raw
        return {"digest": "sha256:" + digest, "size": len(raw), "mediaType": media_type}

    manifests = []
    for arch in arches:
        config = {"architecture": arch, "os": "linux", "rootfs": {"diff_ids": []}, "config": {"Labels": {
            "org.opencontainers.image.source": candidate.SOURCE,
            "org.opencontainers.image.revision": SHA,
            "org.opencontainers.image.version": "0.3.0",
        }}}
        if mutate:
            mutate(config)
        item = blob({"config": blob(config, "application/vnd.oci.image.config.v1+json"),
                     "layers": [blob(b"layer content", "application/vnd.oci.image.layer.v1.tar")]},
                    "application/vnd.oci.image.manifest.v1+json")
        item["platform"] = {"os": "linux", "architecture": arch}
        manifests.append(item)
    root = blob({"schemaVersion": 2, "manifests": manifests}, "application/vnd.oci.image.index.v1+json")
    files["index.json"] = json.dumps({"schemaVersion": 2, "manifests": [root]}).encode()
    with tarfile.open(path, "w") as archive:
        for name, raw in files.items():
            member = tarfile.TarInfo(name)
            member.size = len(raw)
            archive.addfile(member, io.BytesIO(raw))
    return root


class CandidateTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.archive = self.root / "image.tar"
        self.output = self.root / "checks"
        self.output.mkdir()
        fixture(self.archive)

    @contextmanager
    def verify_commands(self, output, failure=None, failed_arch=None, download_failure=False):
        identity = candidate.inspect_archive(self.archive, "0.3.0", SHA)
        download = io.BytesIO()
        with tarfile.open(fileobj=download, mode="w:gz") as archive:
            member = tarfile.TarInfo("trivy")
            member.size = len(b"fixture scanner")
            archive.addfile(member, io.BytesIO(b"fixture scanner"))
        download_bytes = download.getvalue()
        events = {"loaded": [], "removed": [], "scans": [], "smokes": []}
        images = {}
        containers = {}

        def run(*args, **kwargs):
            self.assertFalse((output / "verified.json").exists(), "PASS was written before gates finished")
            if args[:1] == ("skopeo",) and "copy" in args:
                arch = args[args.index("--override-arch") + 1]
                destination, image = args[-1].removeprefix("docker-archive:").split(":", 1)
                images[image] = arch
                with tarfile.open(self.archive) as original:
                    raw = original.extractfile("blobs/sha256/" + identity["images"][arch]["config_digest"][7:]).read()
                files = {"config.json": raw, "manifest.json": json.dumps([
                    {"Config": "config.json", "RepoTags": [image], "Layers": []}]).encode()}
                with tarfile.open(destination, "w") as archive:
                    for name, value in files.items():
                        member = tarfile.TarInfo(name)
                        member.size = len(value)
                        archive.addfile(member, io.BytesIO(value))
            elif args[:1] == (str(output / "trivy"),):
                arch = args[args.index("--platform") + 1].split("/")[1]
                events["scans"].append(arch)
                report = {"SchemaVersion": 2, "Metadata": {"OS": {"Family": "alpine", "Name": "3.24.1"},
                          "ImageID": identity["images"][arch]["config_digest"],
                          "ImageConfig": identity["images"][arch]["config"]},
                          "Results": [{"Class": "os-pkgs", "Type": "alpine",
                                       "Packages": [{"Name": "libssl3"}]}]}
                if arch == failed_arch and failure in ("scan-command", "scan-report"):
                    report["Results"][0]["Vulnerabilities"] = [{"Severity": "HIGH"}]
                Path(args[args.index("--output") + 1]).write_text(json.dumps(report))
                if arch == failed_arch and failure == "scan-command":
                    raise subprocess.CalledProcessError(1, args)
            elif args[:2] == ("docker", "load"):
                with tarfile.open(args[-1]) as archive:
                    image = json.load(archive.extractfile("manifest.json"))[0]["RepoTags"][0]
                events["loaded"].append(image)
            elif args[:3] == ("docker", "image", "inspect"):
                arch = images[args[-1]]
                expected = identity["images"][arch]
                config = copy.deepcopy(expected["config"]["config"])
                config.update({key: value for key, value in candidate.INSPECT_DEFAULTS.items() if key not in config})
                if arch == failed_arch and failure == "loaded-image":
                    config["Labels"]["org.opencontainers.image.revision"] = "wrong"
                return json.dumps([{"Id": expected["config_digest"], "Os": "linux", "Architecture": arch,
                                    "Config": config,
                                    "RootFS": {"Layers": expected["config"]["rootfs"]["diff_ids"]}}]).encode()
            elif args[:2] == ("docker", "create"):
                arch = args[args.index("--platform") + 1].split("/")[1]
                self.assertEqual(args[-1], identity["images"][arch]["config_digest"])
                containers[args[args.index("--name") + 1]] = arch
            elif args[:2] == ("docker", "cp"):
                arch = containers[args[-2].split(":", 1)[0]]
                Path(args[-1]).write_bytes(b"fixture binary: " + arch.encode())
            elif args[:2] == ("docker", "rm"):
                del containers[args[-1]]
            elif args[:3] == ("docker", "image", "rm"):
                self.assertIn(args[-1], events["loaded"])
                events["removed"].append(args[-1])
            elif args[:3] == ("go", "version", "-m"):
                arch = Path(args[-1]).name.removesuffix("-mqlite")
                if arch == failed_arch and failure == "build-info":
                    arch = "arm64" if arch == "amd64" else "amd64"
                return build_info(arch).encode()
            elif len(args) > 1 and args[1] == str(ROOT / "test/release_image_smoke.py"):
                arch = args[args.index("--platform") + 1].split("/")[1]
                self.assertEqual(args[2], identity["images"][arch]["config_digest"])
                events["smokes"].append(arch)
                failed = arch == failed_arch and failure == "smoke"
                kwargs["log"].write_text("FAIL fixture smoke\n" if failed else "PASS fixture smoke\n")
                if failed:
                    raise subprocess.CalledProcessError(1, args)
            else:
                self.fail("unexpected external command: " + repr(args))

        downloads = ([urllib.error.HTTPError("https://github.com/fixture", 504, "Gateway Timeout", {}, None)]
                     if download_failure else []) + [io.BytesIO(download_bytes)]
        with patch.object(candidate, "open_trivy", side_effect=downloads), \
                patch.object(candidate.time, "sleep"), \
                patch.object(candidate, "TRIVY_SHA256", candidate.digest(download_bytes)), \
                patch.object(candidate, "command", side_effect=run), patch("builtins.print"), \
                patch.object(candidate, "datetime") as clock:
            clock.now.return_value = datetime.combine(SCAN_DATE, datetime.min.time(), candidate.timezone.utc)
            yield identity, events
        self.assertFalse(containers, "binary extraction container was not cleaned up")

    def test_verify_pass_requires_both_platforms_and_complete_evidence(self):
        output = self.root / "verify-pass"
        with self.verify_commands(output) as (identity, events):
            candidate.verify(self.archive, output, "0.3.0", SHA)
        self.assertEqual(json.loads((output / "verified.json").read_text()), dict(identity, status="PASS"))
        self.assertFalse((output / "verified.json.tmp").exists())
        binaries = json.loads((output / "binaries.json").read_text())
        self.assertEqual(set(binaries), {"amd64", "arm64"})
        for arch in ("amd64", "arm64"):
            self.assertEqual(binaries[arch]["binary_sha256"], candidate.digest(b"fixture binary: " + arch.encode()))
            self.assertTrue((output / (arch + "-os-scan.json")).is_file())
            self.assertEqual((output / (arch + "-smoke.log")).read_text(), "PASS fixture smoke\n")
        self.assertEqual(events["scans"], ["amd64", "arm64"])
        self.assertEqual(events["smokes"], ["amd64", "arm64"])
        self.assertEqual(len(events["loaded"]), 2)
        self.assertEqual(events["removed"], events["loaded"])

    def test_verify_recovers_from_scanner_download_gateway_timeout(self):
        output = self.root / "verify-download-retry"
        with self.verify_commands(output, download_failure=True) as (_, events):
            candidate.verify(self.archive, output, "0.3.0", SHA)
        self.assertEqual(events["scans"], ["amd64", "arm64"])
        self.assertEqual(events["smokes"], ["amd64", "arm64"])
        self.assertEqual(json.loads((output / "verified.json").read_text())["status"], "PASS")

    def test_failed_or_unverified_download_never_reaches_scan_or_writes_pass(self):
        for failure in ("exhausted", "checksum", "budget", "certificate"):
            with self.subTest(failure=failure):
                output = self.root / ("download-" + failure)
                with self.verify_commands(output) as (_, events):
                    if failure == "exhausted":
                        download = [urllib.error.HTTPError(candidate.TRIVY_URL, 504, "fixture", {}, None)
                                    for _ in range(3)]
                    elif failure == "checksum":
                        download = [io.BytesIO(b"wrong archive")]
                    elif failure == "budget":
                        download = candidate.DownloadBudgetExpired("fixture deadline")
                    else:
                        download = ssl.SSLCertVerificationError("fixture certificate")
                    with patch.object(candidate, "open_trivy", side_effect=download):
                        with self.assertRaises((RuntimeError, ssl.SSLCertVerificationError)):
                            candidate.verify(self.archive, output, "0.3.0", SHA)
                self.assertFalse((output / "verified.json").exists())
                self.assertFalse((output / "trivy.tar.gz").exists())
                self.assertEqual(list(output.glob(".*.part")), [])
                self.assertEqual(events, {"loaded": [], "removed": [], "scans": [], "smokes": []})

    def test_verify_gate_failures_never_write_pass_and_clean_loaded_images(self):
        for failure in ("scan-command", "scan-report", "loaded-image", "build-info", "smoke"):
            for arch in ("amd64", "arm64"):
                with self.subTest(failure=failure, arch=arch):
                    output = self.root / (failure + "-" + arch)
                    with self.verify_commands(output, failure, arch) as (_, events):
                        expected_error = RuntimeError if failure in ("scan-report", "loaded-image", "build-info") else subprocess.CalledProcessError
                        with self.assertRaises(expected_error):
                            candidate.verify(self.archive, output, "0.3.0", SHA)
                    self.assertFalse((output / "verified.json").exists())
                    self.assertFalse((output / "verified.json.tmp").exists())
                    self.assertEqual(events["scans"], ["amd64"] if arch == "amd64" else ["amd64", "arm64"])
                    completed = [] if arch == "amd64" else ["amd64"]
                    expected_smokes = completed + ([arch] if failure == "smoke" else [])
                    self.assertEqual(events["smokes"], expected_smokes)
                    expected_loaded = len(completed) + (1 if failure in ("loaded-image", "build-info", "smoke") else 0)
                    self.assertEqual(len(events["loaded"]), expected_loaded)
                    self.assertEqual(events["removed"], events["loaded"])

    def test_loaded_image_accepts_only_documented_extra_defaults(self):
        defaults = {
            "Hostname": "", "Domainname": "", "Image": "", "MacAddress": "",
            "AttachStdin": False, "AttachStdout": False, "AttachStderr": False,
            "Tty": False, "OpenStdin": False, "StdinOnce": False,
            "NetworkDisabled": False, "StopTimeout": None,
            "Cmd": None, "Entrypoint": None, "Env": None, "Labels": None,
            "OnBuild": None, "Volumes": None, "User": "", "WorkingDir": "",
        }
        self.assertEqual(candidate.INSPECT_DEFAULTS, defaults)
        for arch in ("amd64", "arm64"):
            expected = {"config": {}, "rootfs": {"diff_ids": ["sha256:first", "sha256:second"]}}
            actual = {"Config": {}, "RootFS": {"Layers": expected["rootfs"]["diff_ids"]},
                      "Os": "linux", "Architecture": arch}
            candidate.check_loaded_image(actual, expected, arch)
            actual["Config"] = copy.deepcopy(defaults)
            candidate.check_loaded_image(actual, expected, arch)
            self.assertEqual(actual["Config"], defaults, "comparison mutated inspect evidence")
            for key, default in defaults.items():
                for wrong in (True, 0, "unexpected", [], {}):
                    with self.subTest(arch=arch, field=key, wrong=wrong):
                        actual["Config"] = {key: wrong}
                        with self.assertRaisesRegex(RuntimeError, "Config." + key):
                            candidate.check_loaded_image(actual, expected, arch)
                # Defaults present in the source are part of its exact identity too.
                expected["config"] = {key: default}
                actual["Config"] = {key: default}
                candidate.check_loaded_image(actual, expected, arch)
                actual["Config"] = {}
                with self.assertRaisesRegex(RuntimeError, "Config." + key):
                    candidate.check_loaded_image(actual, expected, arch)
                expected["config"] = {}
            for value in (None, False, "", 0):
                actual["Config"] = {"UnexpectedField": value}
                with self.assertRaisesRegex(RuntimeError, "Config.UnexpectedField"):
                    candidate.check_loaded_image(actual, expected, arch)

    def test_loaded_image_rejects_every_behavior_platform_and_layer_change(self):
        config = {"Env": ["MQLITE_DB=/data/queue.db", "MQLITE_SYNC=FULL"],
                  "Entrypoint": ["/usr/local/bin/mqlite"], "Cmd": ["serve"],
                  "WorkingDir": "/data", "User": "1000", "Labels": {"revision": SHA},
                  "ExposedPorts": {"6754/tcp": {}}, "Volumes": {"/data": {}},
                  "StopSignal": "SIGTERM", "Healthcheck": {"Test": ["CMD", "true"]},
                  "ArgsEscaped": False, "OnBuild": ["RUN true"], "Shell": ["/bin/sh", "-c"]}
        for arch in ("amd64", "arm64"):
            expected = {"config": config, "rootfs": {"diff_ids": ["sha256:first", "sha256:second"]}}
            original = {"Config": copy.deepcopy(config), "Os": "linux", "Architecture": arch,
                        "RootFS": {"Layers": list(expected["rootfs"]["diff_ids"])}}
            candidate.check_loaded_image(original, expected, arch)
            for key in config:
                for operation in ("remove", "change"):
                    actual = copy.deepcopy(original)
                    if operation == "remove":
                        del actual["Config"][key]
                    else:
                        actual["Config"][key] = "changed-secret-do-not-print"
                    with self.subTest(arch=arch, field=key, operation=operation):
                        with self.assertRaisesRegex(RuntimeError, "Config." + key) as error:
                            candidate.check_loaded_image(actual, expected, arch)
                        self.assertNotIn("changed-secret-do-not-print", str(error.exception))
            for key, wrong in (("Os", "windows"), ("Architecture", "arm64" if arch == "amd64" else "amd64")):
                actual = copy.deepcopy(original)
                actual[key] = wrong
                with self.assertRaisesRegex(RuntimeError, key):
                    candidate.check_loaded_image(actual, expected, arch)
            for layers in ([], ["sha256:first"], ["sha256:second", "sha256:first"],
                           ["sha256:first", "sha256:second", "sha256:extra"]):
                actual = copy.deepcopy(original)
                actual["RootFS"]["Layers"] = layers
                with self.assertRaisesRegex(RuntimeError, "RootFS.Layers"):
                    candidate.check_loaded_image(actual, expected, arch)

    def test_binary_build_info_requires_each_setting_once_for_each_platform(self):
        for arch in ("amd64", "arm64"):
            valid = build_info(arch)
            candidate.check_build_info(valid, arch)
            fields = (("path", "github.com/mqlitehq/mqlite/cmd/mqlite", "example.com/wrong"),
                      ("GOOS", "linux", "windows"),
                      ("GOARCH", arch, "arm64" if arch == "amd64" else "amd64"),
                      ("CGO_ENABLED", "0", "1"))
            for key, expected, wrong in fields:
                line = "\tpath\t" + expected + "\n" if key == "path" else "\tbuild\t" + key + "=" + expected + "\n"
                for mutation, changed in (("missing", valid.replace(line, "")),
                                          ("duplicate", valid + line),
                                          ("wrong", valid.replace(line, line.replace(expected, wrong)))):
                    with self.subTest(arch=arch, key=key, mutation=mutation):
                        with self.assertRaises(RuntimeError):
                            candidate.check_build_info(changed, arch)

    def test_verify_binary_evidence_write_failure_cannot_leave_pass(self):
        output = self.root / "verify-write-failure"
        write_text = Path.write_text

        def fail_binary_evidence(path, *args, **kwargs):
            if path == output / "binaries.json":
                raise OSError("fixture disk full writing binaries.json")
            return write_text(path, *args, **kwargs)

        with self.verify_commands(output) as (_, events), \
                patch.object(Path, "write_text", fail_binary_evidence):
            with self.assertRaisesRegex(OSError, "disk full writing binaries.json"):
                candidate.verify(self.archive, output, "0.3.0", SHA)
        self.assertEqual(events["smokes"], ["amd64", "arm64"])
        self.assertEqual(len(events["loaded"]), 2)
        self.assertEqual(events["removed"], events["loaded"])
        self.assertFalse((output / "verified.json").exists())
        self.assertFalse((output / "verified.json.tmp").exists())

    def test_smoke_run_timeout_hides_generated_token_and_cleans_resources(self):
        calls = []
        args = SimpleNamespace(docker="docker", image="fixture-image", platform="linux/arm64",
                               expected_version="0.3.0", expected_revision=SHA)
        labels = {"org.opencontainers.image." + name: value for name, value in (
            ("source", candidate.SOURCE), ("version", "0.3.0"), ("revision", SHA))}

        def run(argv, **kwargs):
            calls.append(argv)
            stdout = ""
            if argv[1:3] == ["image", "inspect"]:
                stdout = json.dumps([{"Os": "linux", "Architecture": "arm64", "Config": {"Labels": labels}}])
            elif argv[1] == "run":
                raise subprocess.TimeoutExpired(argv, kwargs["timeout"])
            elif argv[1:3] != ["volume", "create"] and argv[1] not in ("logs", "rm", "volume"):
                self.fail("unexpected smoke command: " + repr(argv))
            return subprocess.CompletedProcess(argv, 0, stdout=stdout, stderr="")

        with patch.object(release_smoke.subprocess, "run", side_effect=run), patch("builtins.print") as output:
            with self.assertRaisesRegex(RuntimeError, "docker run timed out after 60 seconds") as failure:
                release_smoke.smoke(args)
        self.assertNotIn("mqk_", str(failure.exception))
        self.assertNotIn("MQLITE_TOKENS", str(failure.exception))
        self.assertTrue(failure.exception.__suppress_context__)
        printed = " ".join(str(call.args) for call in output.call_args_list)
        self.assertNotIn("mqk_", printed)
        self.assertNotIn("MQLITE_TOKENS", printed)
        started = next(call for call in calls if call[1] == "run")
        self.assertTrue(any(value.startswith("MQLITE_TOKENS=mqk_") for value in started))
        container = started[started.index("--name") + 1]
        volume = next(call[-1] for call in calls if call[1:3] == ["volume", "create"])
        self.assertIn(["docker", "rm", "--force", container], calls)
        self.assertIn(["docker", "volume", "rm", "--force", volume], calls)

    def test_complete_oci_inventory_and_every_provenance_field(self):
        actual = candidate.inspect_archive(self.archive, "0.3.0", SHA)
        self.assertEqual(set(actual["images"]), {"amd64", "arm64"})
        for arches in (("amd64",), ("arm64",), ("amd64", "amd64"), ("amd64", "arm64", "riscv64"), ()):
            with self.subTest(arches=arches):
                fixture(self.archive, arches)
                with self.assertRaises(RuntimeError):
                    candidate.inspect_archive(self.archive, "0.3.0", SHA)
        for field in ("source", "version", "revision"):
            with self.subTest(field=field):
                fixture(self.archive, mutate=lambda cfg: cfg["config"]["Labels"].update({
                    "org.opencontainers.image." + field: "wrong"}))
                with self.assertRaises(RuntimeError):
                    candidate.inspect_archive(self.archive, "0.3.0", SHA)
        for field, value in (("architecture", "riscv64"), ("os", "windows")):
            fixture(self.archive, mutate=lambda cfg: cfg.update({field: value}))
            with self.assertRaises(RuntimeError):
                candidate.inspect_archive(self.archive, "0.3.0", SHA)

    def test_rehashed_graph_and_corrupted_blob_are_distinct_failures(self):
        raw = self.archive.read_bytes()
        self.assertIn(b"layer content", raw)
        self.archive.write_bytes(raw.replace(b"layer content", b"LAYER CONTENT"))
        with self.assertRaisesRegex(RuntimeError, "blob size/digest"):
            candidate.inspect_archive(self.archive, "0.3.0", SHA)

    def test_unknown_empty_wrong_platform_and_vulnerable_scans_fail_closed(self):
        clean = {"SchemaVersion": 2, "Metadata": {"OS": {"Family": "alpine", "Name": "3.24.1"},
                 "ImageID": "sha256:config", "ImageConfig": {"architecture": "arm64", "os": "linux"}},
                 "Results": [{"Class": "os-pkgs", "Type": "alpine", "Packages": [{"Name": "libssl3"}]}]}
        candidate.check_scan(clean, "arm64", "sha256:config", today=SCAN_DATE)
        mutations = [
            lambda r: r.update(SchemaVersion=1),
            lambda r: r["Metadata"]["OS"].update(Family="unknown"),
            lambda r: r["Metadata"]["OS"].update(Name=""),
            lambda r: r["Metadata"]["OS"].update(Name="3.23.0"),
            lambda r: r["Metadata"]["OS"].update(Name="3.25.0"),
            lambda r: r["Metadata"]["OS"].update(EOSL=True),
            lambda r: r["Metadata"].update(ImageID="sha256:other"),
            lambda r: r["Metadata"]["ImageConfig"].update(architecture="amd64"),
            lambda r: r["Metadata"]["ImageConfig"].update(os="windows"),
            lambda r: r.update(Results=[]),
            lambda r: r["Results"][0].update(Packages=[]),
            lambda r: r["Results"][0].update(Class="lang-pkgs"),
            lambda r: r["Results"][0].update(Type="other"),
        ]
        for severity in ("HIGH", "CRITICAL"):
            bad = copy.deepcopy(clean)
            bad["Results"][0]["Vulnerabilities"] = [{"Severity": severity}]
            with self.assertRaises(RuntimeError):
                candidate.check_scan(bad, "arm64", "sha256:config", today=SCAN_DATE)
        for mutation in mutations:
            bad = copy.deepcopy(clean)
            mutation(bad)
            with self.assertRaises(RuntimeError):
                candidate.check_scan(bad, "arm64", "sha256:config", today=SCAN_DATE)
        candidate.check_scan(clean, "arm64", "sha256:config",
                             today=candidate.ALPINE_END_OF_SUPPORT - timedelta(days=1))
        for today in (candidate.ALPINE_END_OF_SUPPORT, candidate.ALPINE_END_OF_SUPPORT + timedelta(days=1)):
            with self.subTest(today=today):
                with self.assertRaisesRegex(RuntimeError, "no longer supported"):
                    candidate.check_scan(clean, "arm64", "sha256:config", today=today)

    def test_no_publish_after_missing_failed_or_changed_candidate(self):
        identity = candidate.inspect_archive(self.archive, "0.3.0", SHA)
        for status, change in ((None, False), ("FAIL", False), ("PASS", True)):
            if status:
                evidence = dict(identity, status=status)
                if change:
                    evidence["archive_sha256"] = "0" * 64
                (self.output / "verified.json").write_text(json.dumps(evidence))
            with patch.object(candidate, "command") as run:
                with self.assertRaises((RuntimeError, FileNotFoundError)):
                    candidate.publish(self.archive, self.output, "0.3.0", SHA, "")
                run.assert_not_called()

    def test_stable_and_rc_publish_only_the_original_index_and_allowed_tags(self):
        identity = candidate.inspect_archive(self.archive, "0.3.0", SHA)
        with tarfile.open(self.archive) as archive:
            raw_index = archive.extractfile("blobs/sha256/" + identity["index_digest"][7:]).read()
        image = "ghcr.io/mqlitehq/mqlite"
        for version, tags in (("0.3.0", [image + ":0.3.0", image + ":0.3", image + ":latest"]),
                              ("0.3.0-rc.1", [image + ":0.3.0-rc.1"])):
            identity["version"] = version
            (self.output / "verified.json").write_text(json.dumps(dict(identity, status="PASS")))
            with patch.dict("os.environ", GITHUB_REPOSITORY="mqlitehq/mqlite"), \
                    patch.object(candidate, "inspect_archive", return_value=identity), \
                    patch("builtins.print"), \
                    patch.object(candidate, "command", return_value=raw_index) as run:
                candidate.publish(self.archive, self.output, version, SHA, ",".join(tags))
                self.assertEqual(run.call_count, len(tags) * 2)
                for index, tag in enumerate(tags):
                    self.assertEqual(run.call_args_list[index * 2].args,
                                     ("skopeo", "copy", "--all", "--preserve-digests",
                                      "oci-archive:" + str(self.archive), "docker://" + tag))
                for bad in ([], tags + [image + ":extra"], [image + ":latest"], tags * 2):
                    run.reset_mock()
                    with self.assertRaises(RuntimeError):
                        candidate.publish(self.archive, self.output, version, SHA, ",".join(bad))
                    run.assert_not_called()
                run.return_value = b"changed remote index"
                with self.assertRaisesRegex(RuntimeError, "published index digest mismatch"):
                    candidate.publish(self.archive, self.output, version, SHA, ",".join(tags))


if __name__ == "__main__":
    unittest.main()
