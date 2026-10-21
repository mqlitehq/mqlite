#!/usr/bin/env python3
"""Offline negative controls for the v0.3.2/v0.4.0 compatibility drill."""

import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import zipfile

sys.dont_write_bytecode = True
import compat  # noqa: E402  (same-directory script import is intentional)


class CompatibilityGateTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.binary = self.root / "mqlite"
        self.binary.write_bytes(b"published binary")

    def tar_archive(self, name="release.tar.gz", members=None):
        path = self.root / name
        if members is None:
            members = [("mqlite", b"published binary", None)]
        with tarfile.open(path, "w:gz") as archive:
            for member_name, contents, kind in members:
                item = tarfile.TarInfo(member_name)
                if kind == "symlink":
                    item.type = tarfile.SYMTYPE
                    item.linkname = "elsewhere"
                    archive.addfile(item)
                else:
                    item.size = len(contents)
                    archive.addfile(item, io.BytesIO(contents))
        return path

    def zip_archive(self):
        path = self.root / "release.zip"
        with zipfile.ZipFile(path, "w") as archive:
            archive.writestr("mqlite.exe", b"published binary")
        return path

    def checksums(self, archive, checksum=None):
        path = self.root / "checksums.txt"
        value = checksum or compat.digest(archive)
        path.write_text(value + "  " + archive.name + "\n")
        return path

    def verify_fixture(self, archive, binary=None, checksum=None):
        checksums = self.checksums(archive, checksum)
        with patch.object(compat, "V032_CHECKSUMS_SHA256", compat.digest(checksums)):
            return compat.verify_old_release(binary or self.binary, archive, checksums)

    def test_valid_tar_and_zip_release_artifacts(self):
        for archive in (self.tar_archive(), self.zip_archive()):
            with self.subTest(archive=archive.name):
                identity = self.verify_fixture(archive)
                self.assertEqual(identity["archive_sha256"], compat.digest(archive))
                self.assertEqual(identity["binary_sha256"], compat.digest(self.binary))
                self.assertEqual(identity["binary_member"],
                                 "mqlite.exe" if archive.suffix == ".zip" else "mqlite")

    def test_checksums_file_requires_official_pinned_hash(self):
        archive = self.tar_archive()
        checksums = self.checksums(archive)
        with self.assertRaisesRegex(RuntimeError, "published v0.3.2 checksums"):
            compat.verify_old_release(self.binary, archive, checksums)

    def test_changed_archive_is_rejected_before_open(self):
        archive = self.tar_archive()
        original_hash = compat.digest(archive)
        checksums = self.checksums(archive, original_hash)
        with archive.open("ab") as stream:
            stream.write(b"changed after checksumming")
        with patch.object(compat, "V032_CHECKSUMS_SHA256", compat.digest(checksums)):
            with patch.object(compat, "release_binary",
                              side_effect=AssertionError("archive must not be opened")):
                with self.assertRaisesRegex(RuntimeError, "archive checksum"):
                    compat.verify_old_release(self.binary, archive, checksums)

    def test_release_member_must_be_unique_regular_and_match_binary(self):
        cases = {
            "missing": [("other", b"published binary", None)],
            "duplicate": [("mqlite", b"published binary", None),
                          ("mqlite", b"published binary", None)],
            "mixed-duplicate": [("mqlite", b"published binary", None),
                                ("mqlite", b"", "symlink")],
            "symlink": [("mqlite", b"", "symlink")],
        }
        for label, members in cases.items():
            with self.subTest(label=label):
                archive = self.tar_archive(label + ".tar.gz", members)
                with self.assertRaisesRegex(RuntimeError, "exactly one regular mqlite"):
                    self.verify_fixture(archive)
        archive = self.tar_archive("mismatch.tar.gz")
        wrong = self.root / "wrong-mqlite"
        wrong.write_bytes(b"locally rebuilt binary")
        with self.assertRaisesRegex(RuntimeError, "does not exactly match"):
            self.verify_fixture(archive, binary=wrong)

    def test_versions_are_exact(self):
        compat.require_version("mqlite 0.4.0", compat.NEW_VERSION, "candidate")
        for version in ("mqlite 0.3.3", "mqlite 0.4.0-rc.1", "mqlite 0.4.1", "0.4.0"):
            with self.subTest(version=version):
                with self.assertRaisesRegex(RuntimeError, "exactly mqlite 0.4.0"):
                    compat.require_version(version, compat.NEW_VERSION, "candidate")


class SourceArchiveTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def git(self, repo, *args):
        return subprocess.check_output(["git", *args], cwd=repo, text=True).strip()

    def repository(self, version="0.4.0"):
        repo = self.root / ("repo-" + version.replace(".", "-"))
        repo.mkdir()
        subprocess.run(["git", "init", "-q"], cwd=repo, check=True)
        source = repo / "internal/version/version.go"
        source.parent.mkdir(parents=True)
        source.write_text('package version\nconst Version = "' + version + '"\n')
        (repo / "payload.txt").write_text("committed\n")
        subprocess.run(["git", "add", "."], cwd=repo, check=True)
        subprocess.run(["git", "-c", "user.name=compat-test",
                        "-c", "user.email=compat@example.invalid",
                        "commit", "-qm", "fixture"], cwd=repo, check=True)
        return repo

    def test_candidate_rejects_tracked_and_untracked_dirty_state(self):
        repo = self.repository()
        clean = compat.source_identity(repo, "HEAD", "0.4.0", require_clean=True)
        self.assertEqual(clean["status_porcelain"], "")
        (repo / "payload.txt").write_text("modified\n")
        with self.assertRaisesRegex(RuntimeError, "candidate source is dirty"):
            compat.source_identity(repo, "HEAD", "0.4.0", require_clean=True)
        subprocess.run(["git", "checkout", "--", "payload.txt"], cwd=repo, check=True)
        (repo / "untracked.txt").write_text("untracked\n")
        with self.assertRaisesRegex(RuntimeError, "candidate source is dirty"):
            compat.source_identity(repo, "HEAD", "0.4.0", require_clean=True)

    def test_candidate_source_version_must_be_exact(self):
        repo = self.repository("0.4.0-rc.1")
        with self.assertRaisesRegex(RuntimeError, "version must be exactly 0.4.0"):
            compat.source_identity(repo, "HEAD", "0.4.0", require_clean=True)

    def test_git_archive_uses_commit_not_worktree(self):
        repo = self.repository()
        commit = self.git(repo, "rev-parse", "HEAD")
        tree = self.git(repo, "rev-parse", "HEAD^{tree}")
        (repo / "payload.txt").write_text("modified\n")
        (repo / "untracked.txt").write_text("untracked\n")
        archive, archive_hash = compat.git_archive(repo, commit)
        destination = self.root / "export"
        compat.extract_git_archive(archive, destination)
        self.assertEqual((destination / "payload.txt").read_text(), "committed\n")
        self.assertFalse((destination / "untracked.txt").exists())
        self.assertEqual(archive_hash, hashlib.sha256(archive).hexdigest())
        self.assertRegex(commit, r"^[0-9a-f]{40}$")
        self.assertRegex(tree, r"^[0-9a-f]{40}$")

    def test_source_export_ignores_replace_refs(self):
        repo = self.repository()
        original = self.git(repo, "rev-parse", "HEAD")
        original_tree = self.git(repo, "rev-parse", "HEAD^{tree}")
        (repo / "payload.txt").write_text("forged\n")
        subprocess.run(["git", "add", "payload.txt"], cwd=repo, check=True)
        subprocess.run(["git", "-c", "user.name=compat-test",
                        "-c", "user.email=compat@example.invalid",
                        "commit", "-qm", "replacement"], cwd=repo, check=True)
        replacement = self.git(repo, "rev-parse", "HEAD")
        subprocess.run(["git", "replace", original, replacement], cwd=repo, check=True)
        subprocess.run(["git", "--no-replace-objects", "reset", "--hard", "-q", original],
                       cwd=repo, check=True)
        self.assertEqual(self.git(repo, "show", original + ":payload.txt"), "forged")

        identity = compat.source_identity(repo, "HEAD", "0.4.0", require_clean=True)
        self.assertEqual(identity["commit"], original)
        self.assertEqual(identity["tree"], original_tree)
        archive, _ = compat.git_archive(repo, original)
        destination = self.root / "replace-export"
        compat.extract_git_archive(archive, destination)
        self.assertEqual((destination / "payload.txt").read_text(), "committed\n")

    def test_source_export_ignores_repository_local_attributes(self):
        repo = self.repository()
        (repo / ".git/info/attributes").write_text("payload.txt export-ignore\n")
        commit = self.git(repo, "rev-parse", "HEAD")
        direct = subprocess.check_output(
            ["git", "archive", "--format=tar", commit], cwd=repo)
        with tarfile.open(fileobj=io.BytesIO(direct), mode="r:") as source:
            self.assertNotIn("payload.txt", source.getnames())

        archive, _ = compat.git_archive(repo, commit)
        destination = self.root / "attributes-export"
        compat.extract_git_archive(archive, destination)
        self.assertEqual((destination / "payload.txt").read_text(), "committed\n")

    def test_archive_build_uses_isolated_source_and_fixed_go_environment(self):
        source_archive = io.BytesIO()
        with tarfile.open(fileobj=source_archive, mode="w") as archive:
            item = tarfile.TarInfo("go.mod")
            contents = b"module example.invalid/fixture\n"
            item.size = len(contents)
            archive.addfile(item, io.BytesIO(contents))
        archive_bytes = source_archive.getvalue()
        output = self.root / "candidate"
        log = self.root / "build.log"

        with patch.dict(os.environ, {"GOFLAGS": "-overlay=untrusted.json",
                                     "GOWORK": "/untrusted/go.work"}):
            env = compat.build_environment("local")
        self.assertEqual(env["GOFLAGS"], "-mod=readonly")
        self.assertEqual(env["GOWORK"], "off")

        def fake_build(command, **kwargs):
            self.assertNotEqual(Path(kwargs["cwd"]), self.root)
            self.assertEqual(kwargs["env"]["GOFLAGS"], "-mod=readonly")
            self.assertEqual(kwargs["env"]["GOWORK"], "off")
            self.assertIn("-mod=readonly", command)
            self.assertIn("-buildvcs=false", command)
            self.assertIn("-trimpath", command)
            Path(command[command.index("-o") + 1]).write_bytes(b"built binary")
            return SimpleNamespace(returncode=0, stdout="build output")

        with patch.object(compat, "git_archive",
                          return_value=(archive_bytes, hashlib.sha256(archive_bytes).hexdigest())):
            with patch.object(compat.subprocess, "run", side_effect=fake_build):
                metadata = compat.build_from_revision(
                    self.root, "a" * 40, output, "./cmd/mqlite", log, env, "go version fixture")
        self.assertEqual(metadata["binary_sha256"], compat.digest(output))
        self.assertEqual(metadata["source_archive_sha256"], hashlib.sha256(archive_bytes).hexdigest())
        saved = json.loads(log.read_text())
        self.assertEqual(saved["go_version"], "go version fixture")
        self.assertEqual(saved["environment"]["CGO_ENABLED"], "0")


if __name__ == "__main__":
    unittest.main()
