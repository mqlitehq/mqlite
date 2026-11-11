#!/usr/bin/env python3
"""Temporary local Git repositories only; no database or credential access."""
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parent
TOOL=Path(os.environ.get('SOURCE_MAP_TEST_TOOL',str(ROOT/'verify-source-map.py')))
sys.dont_write_bytecode=True


def git(root,*args):
    return subprocess.check_output(['git','-C',str(root),*args],text=True,stderr=subprocess.DEVNULL).strip()


def sha(path):return hashlib.sha256(path.read_bytes()).hexdigest()


class SourceMapTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(prefix='source-map-fixture-',dir=ROOT)
        self.addCleanup(self.temp.cleanup)
        self.root=Path(self.temp.name);self.source=self.root/'source';self.sdk=self.root/'sdk'
        for p in (self.source,self.sdk):
            p.mkdir();git(p,'init','-q');git(p,'config','user.name','Source guard fixture');git(p,'config','user.email','fixture@users.noreply.github.com')
        workflow=self.source/'.github/workflows/turso-performance-soak.yml';workflow.parent.mkdir(parents=True);workflow.write_text('name: original fixture\n')
        (self.source/'runtime.go').write_text('package product\n')
        (self.source/'go.mod').write_text('module fixture.invalid/product\n\ngo 1.21\n')
        (self.source/'.gitignore').write_text('**/ignored.go\n')
        (self.sdk/'sdk.go').write_text('package fixture\n')
        (self.sdk/'go.mod').write_text('module fixture.invalid/sdk\n\ngo 1.21\n')
        for p in (self.source,self.sdk):git(p,'add','.');git(p,'commit','-qm','Original fixture')
        self.runtime=git(self.source,'rev-parse','HEAD');self.sdk_sha=git(self.sdk,'rev-parse','HEAD')
        runtime_files={n:sha(self.source/n) for n in git(self.source,'ls-files').splitlines()}
        self.support=self.source/'test/m163-shutdown-clients';self.support.mkdir(parents=True)
        shutil.copyfile(TOOL,self.support/'verify-source-map.py')
        (self.support/'main.go').write_text('package main\nfunc main() {}\n')
        (self.support/'go.mod').write_text('module fixture.invalid/diagnostic\n\ngo 1.21\n')
        harness={p.name:sha(p) for p in self.support.iterdir() if p.is_file()}
        workflow=self.source/'.github/workflows/m163-shutdown-clients.yml'
        workflow.write_text('name: reviewed diagnostic fixture\n')
        self.expected={'runtime_sha':self.runtime,'sdk_sha':self.sdk_sha,'runtime_files':runtime_files,
                       'harness_files':harness,'harness_sha256':self.map_sha(harness),'workflow_sha256':sha(workflow)}
        self.save_expected()
        self.commit()
        subprocess.run(['git','clone','--shared',str(self.sdk),str(self.source/'.diagnostic-sdk')],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        spec=importlib.util.spec_from_file_location('source_map_fixture',self.support/'verify-source-map.py')
        self.verifier=importlib.util.module_from_spec(spec);spec.loader.exec_module(self.verifier)

    @staticmethod
    def map_sha(mapping):return hashlib.sha256(json.dumps(mapping,sort_keys=True,separators=(',',':')).encode()).hexdigest()

    def save_expected(self):
        (self.support/'source-map.expected.json').write_text(json.dumps(self.expected,indent=2)+'\n')

    def commit(self):
        git(self.source,'add','--all','--','.',':(exclude).diagnostic-sdk');git(self.source,'commit','-qm','Reviewed diagnostic fixture')

    def check(self,phase='before'):
        env={'M164_EVIDENCE_DIR':str(self.root/'evidence'),'M164_RUNTIME_SHA':self.runtime,
             'M164_SDK_SHA':self.sdk_sha,'M164_HARNESS_SHA':self.expected['harness_sha256'],'GOTOOLCHAIN':'local','GOPROXY':'off','GOSUMDB':'off'}
        with patch.dict(os.environ,env):self.verifier.verify(self.source,phase)

    def reject(self):
        with self.assertRaises(RuntimeError):self.check()

    def assert_go_input(self,path):
        env=os.environ.copy();env.update({'GOTOOLCHAIN':'local','GOPROXY':'off','GOSUMDB':'off','GOMAXPROCS':'2'})
        files=subprocess.check_output(['go','list','-f','{{.GoFiles}}','.'],cwd=path.parent,env=env,text=True).strip()
        self.assertIn(path.name,files)

    def test_committed_extra_go_is_rejected(self):
        p=self.support/'injected.go';p.write_text('package main\nvar injected = 42\n');self.commit()
        self.assert_go_input(p);self.reject()

    def test_staged_extra_go_is_rejected(self):
        p=self.support/'injected.go';p.write_text('package main\nvar injected = 42\n');git(self.source,'add',str(p.relative_to(self.source)))
        self.assert_go_input(p);self.reject()

    def test_clean_before_and_after_pass(self):
        self.check('before');self.check('after')
        for phase in ('before','after'):
            result=json.loads((self.root/'evidence'/('source-map-'+phase+'.json')).read_text())
            self.assertTrue(result['product_and_sdk_verified'])

    def test_ignored_extra_harness_go_is_rejected(self):
        p=self.support/'ignored.go';p.write_text('package main\nvar injected = 42\n')
        git(self.source,'check-ignore',str(p.relative_to(self.source)));self.assert_go_input(p);self.reject()

    def test_ignored_extra_product_go_is_rejected(self):
        p=self.source/'ignored.go';p.write_text('package product\nvar injected = 42\n')
        git(self.source,'check-ignore','ignored.go');self.assert_go_input(p);self.reject()

    def test_unstaged_runtime_modification_is_rejected(self):
        (self.source/'runtime.go').write_text('package product\nvar changed = 1\n');self.reject()

    def test_staged_runtime_modification_is_rejected(self):
        (self.source/'runtime.go').write_text('package product\nvar changed = 1\n');git(self.source,'add','runtime.go');self.reject()

    def test_hidden_runtime_change_and_rewritten_manifest_is_rejected(self):
        original=git(self.source,'show','HEAD:runtime.go')
        git(self.source,'update-index','--assume-unchanged','runtime.go')
        p=self.source/'runtime.go';p.write_text('package product\nvar modified = 1\n')
        self.expected['runtime_files']['runtime.go']=sha(p);self.save_expected();self.commit()
        self.assertEqual(git(self.source,'show','HEAD:runtime.go'),original)
        self.assertEqual(git(self.source,'diff','HEAD','--name-only'),'')
        self.assert_go_input(p);self.reject()

    def test_modified_harness_is_rejected(self):
        (self.support/'main.go').write_text('package main\nfunc main() {println(42)}\n');self.reject()

    def test_symlink_harness_file_is_rejected_even_same_bytes(self):
        p=self.support/'main.go';outside=self.root/'outside.go';outside.write_bytes(p.read_bytes());p.unlink();p.symlink_to(outside);self.commit();self.reject()

    def test_symlink_harness_directory_is_rejected(self):
        outside=self.root/'outside-dir';outside.mkdir();(self.support/'extra').symlink_to(outside,target_is_directory=True);self.commit();self.reject()

    def test_extra_committed_non_go_file_is_rejected(self):
        (self.support/'extra.txt').write_text('unreviewed\n');self.commit();self.reject()

    def test_manifest_cannot_add_file_without_changing_pinned_digest(self):
        p=self.support/'injected.go';p.write_text('package main\nvar injected = 42\n')
        self.expected['harness_files']['injected.go']=sha(p);self.save_expected();self.commit();self.reject()

    def test_modified_sdk_is_rejected(self):
        (self.source/'.diagnostic-sdk/sdk.go').write_text('package fixture\nvar modified = 1\n');self.reject()

    def test_sdk_hidden_index_flag_modification_is_rejected(self):
        nested=self.source/'.diagnostic-sdk';git(nested,'update-index','--assume-unchanged','sdk.go')
        (nested/'sdk.go').write_text('package fixture\nvar modified = 1\n')
        self.assertEqual(git(nested,'status','--porcelain'),'');self.reject()

    def test_sdk_different_head_same_tree_is_rejected(self):
        nested=self.source/'.diagnostic-sdk';git(nested,'config','user.name','Source guard fixture');git(nested,'config','user.email','fixture@users.noreply.github.com')
        git(nested,'commit','--allow-empty','-qm','Different SDK identity');self.reject()

    def test_ignored_sdk_go_is_rejected(self):
        nested=self.source/'.diagnostic-sdk';(nested/'.git/info/exclude').write_text('ignored.go\n');(nested/'ignored.go').write_text('package fixture\nvar injected = 42\n');self.reject()

    def test_evidence_inside_source_is_rejected(self):
        env={'M164_EVIDENCE_DIR':str(self.source/'evidence'),'M164_RUNTIME_SHA':self.runtime,'M164_SDK_SHA':self.sdk_sha,'M164_HARNESS_SHA':self.expected['harness_sha256']}
        with patch.dict(os.environ,env):
            with self.assertRaises(RuntimeError):self.verifier.verify(self.source,'before')


if __name__=='__main__':unittest.main()
