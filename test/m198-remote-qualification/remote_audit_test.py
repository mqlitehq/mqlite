#!/usr/bin/env python3
"""Offline negative controls for the independent remote artifact reader."""
import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest
from unittest import mock

specification = importlib.util.spec_from_file_location("remote", Path(__file__).with_name("run-remote.py"))
remote = importlib.util.module_from_spec(specification)
specification.loader.exec_module(remote)


def fixture():
    result = {"passed": True, "close_joined": True, "heap_growth": 0, "goroutine_growth": 0, "rows": [], "rpc_calls": {}}
    ledger = {"pass": True, "resources": [], "identities": [], "rpc_trace": []}
    sequence = 0
    for client in ("native", "sdk"):
        for batch in (1, 8):
            queue = "m153-control-" + client + "-" + str(batch)
            ledger["resources"].append({"name": queue, "create_acknowledged": True, "deletion_confirmed": True, "confirmed_absent": True})
            result["rows"].append({"profile":"plain","client": client, "batch": batch, "cycles": 3, "message_count": batch * 3, "empty": True, "deleted": True})
            result["rpc_calls"][queue] = {"Send": 3, "Receive": 3, "CompleteBatch": 3}
            for cycle in range(3):
                for slot in range(batch):
                    sequence += 1
                    name = queue + "-" + str(cycle) + "-" + str(slot)
                    body = (name.encode() + b":" + b"p" * 256)[:256]
                    ledger["identities"].append({"id": name, "queue": queue, "sequence": sequence, "body_sha256": hashlib.sha256(body).hexdigest(),"metadata_profile":"plain","metadata_sha256":remote.expected_metadata_hash("plain",name), "acknowledged": True, "completed": True, "deliveries": 1})
                    ledger["rpc_trace"].append({"operation": "CompleteBatch", "queue": queue, "sequence": sequence, "http_status": 200, "confirmed": True})
    for row in result["rows"]:
        digest=hashlib.sha256()
        for identity in ledger["identities"]:
            if identity["queue"].endswith("-"+row["client"]+"-"+str(row["batch"])):
                digest.update((identity["id"]+":"+str(identity["sequence"])+":"+identity["body_sha256"]+":"+identity["metadata_sha256"]+"\n").encode())
        row["identity_sequence_body_sha256"]=digest.hexdigest()
    result["resource_samples"] = [{"elapsed_seconds": 0, "confirmed_identities": 0}, {"elapsed_seconds": 1, "confirmed_identities": len(ledger["identities"])}]
    return result, ledger


class ArtifactAudit(unittest.TestCase):
    def test_complete_control(self):
        remote.audit_stage(*fixture(), "controls")

    def test_eleven_corruptions_are_rejected(self):
        for fault in ("missing-id", "duplicate-id", "wrong-body", "sequence", "delivery", "missing-ack", "duplicate-ack", "rpc-replay", "sample-gap", "resource-growth", "cleanup", "metadata", "digest"):
            with self.subTest(fault=fault):
                result, ledger = copy.deepcopy(fixture())
                if fault == "missing-id":
                    ledger["identities"].pop()
                if fault == "duplicate-id":
                    ledger["identities"][1] = copy.deepcopy(ledger["identities"][0])
                if fault == "wrong-body":
                    ledger["identities"][0]["body_sha256"] = "0" * 64
                if fault == "sequence":
                    ledger["identities"][0]["sequence"] = 0
                if fault == "delivery":
                    ledger["identities"][0]["deliveries"] = 2
                if fault == "missing-ack":
                    ledger["rpc_trace"].pop()
                if fault == "duplicate-ack":
                    ledger["rpc_trace"].append(copy.deepcopy(ledger["rpc_trace"][0]))
                if fault == "rpc-replay":
                    next(iter(result["rpc_calls"].values()))["Receive"] += 1
                if fault == "sample-gap":
                    result["resource_samples"][-1]["elapsed_seconds"] = 31
                if fault == "resource-growth":
                    result["heap_growth"] = (16 << 20) + 1
                if fault == "cleanup":
                    ledger["resources"][0]["confirmed_absent"] = False
                if fault == "metadata":
                    ledger["identities"][0]["metadata_sha256"] = "0"*64
                if fault == "digest":
                    result["rows"][0]["identity_sequence_body_sha256"] = "0"*64
                with self.assertRaises(AssertionError):
                    remote.audit_stage(result, ledger, "controls")

    def test_controller_budget_reserves_cleanup_before_child(self):
        with mock.patch.object(remote.time,"monotonic",return_value=100), mock.patch.object(remote.supervisor,"execute",return_value={"exit":0,"cleanup":{"group_gone":True,"leader_joined":True}}) as child:
            with self.assertRaises(TimeoutError):remote.bounded_execute(["control"],310,{},414.999)
            child.assert_not_called()
            self.assertEqual(remote.bounded_execute(["control"],310,{},415)["exit"],0)
            child.assert_called_once_with(["control"],310,{})
        with self.assertRaises(TimeoutError):remote.overall_deadline(None,None)

    def test_outlet_control_and_corruptions(self):
        directory=Path(__file__).with_name("reader-controls")
        valid=json.loads((directory/"outlets.json").read_text())
        remote.audit_outlets(valid,directory)
        for fault in ("passed","harness_completed","owner_joined","physical_absence_confirmed","cleanup-budget","physical-count","physical-query","missing-client","missing-outlet","metadata","expected-and-actual-forgery","missing-proof","delivery-count","sequence","replay-fence","retained-state","operation-ack"):
            with self.subTest(fault=fault):
                bad=copy.deepcopy(valid)
                if fault in ("passed","harness_completed","owner_joined","physical_absence_confirmed"):bad[fault]=False
                if fault=="cleanup-budget":bad["cleanup_elapsed_seconds"]=60.01
                if fault=="physical-count":bad["physical_rows"][0]["message_rows"]=1
                if fault=="physical-query":bad["physical_rows"][0]["query_succeeded"]=False
                if fault=="missing-client":del bad["clients"]["sdk"]
                if fault=="missing-outlet":bad["clients"]["sdk"]["outlets"]["dlq"]=False
                if fault=="metadata":bad["observations"][0]["actual_sha256"]["correlation_id"]="0"*64
                if fault=="expected-and-actual-forgery":
                    bad["observations"][0]["actual_sha256"]["correlation_id"]="0"*64;bad["observations"][0]["expected_sha256"]["correlation_id"]="0"*64
                if fault=="missing-proof":bad["observations"].pop()
                if fault=="delivery-count":bad["observations"][0]["delivery_count"]=1
                if fault=="sequence":bad["observations"][0]["sequence"]+=100
                if fault=="replay-fence":
                    row=next(row for row in bad["observations"] if row["outlet"]=="attempt_replay")
                    row["expected_sha256"]["fence"]=row["actual_sha256"]["fence"]="0"*64
                if fault=="retained-state":bad["retained_states"]["native"]["total"]=0
                if fault=="operation-ack":bad["confirmed_operations"].pop()
                with self.assertRaises((AssertionError,KeyError)):remote.audit_outlets(bad,directory)

    def test_provider_evidence_rejects_errors_and_missing_observation(self):
        valid={"events":[],"dropped":0,"grace_completed":True,"passive_grace_seconds":10,"server_stream_absence_verified":False}
        remote.audit_provider(valid,outlet=True)
        for key,value in (("events",[{"http_status":400}]),("dropped",1),("grace_completed",False),("passive_grace_seconds",0),("server_stream_absence_verified",True)):
            bad=copy.deepcopy(valid);bad[key]=value
            with self.assertRaises(AssertionError):remote.audit_provider(bad,outlet=True)


if __name__ == "__main__":
    unittest.main()
