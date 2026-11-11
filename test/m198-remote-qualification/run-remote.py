#!/usr/bin/env python3
"""Scoped final M197 qualification: comparable plain timing; strict B escaped correctness and outlets; independent sustained workloads."""
import argparse
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import signal
import statistics
import tempfile
import time

HERE = Path(__file__).resolve().parent


def module(name, path):
    specification = importlib.util.spec_from_file_location(name, path)
    loaded = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(loaded)
    return loaded


local = module("local_qualification", HERE / "source_helpers.py")
supervisor = module("opaque_supervisor", HERE / "harness/run-safe.py")


def overall_deadline(signum, frame):
    raise TimeoutError("Frozen1800-second controller deadline includes archives/build/controls/workloads/sourcecleanup and five-second cleanup reserve")

def bounded_execute(command,timeout,environment,deadline):
    if deadline-time.monotonic() < timeout+5:
        raise TimeoutError("Remaining overall budget cannot contain the full original child deadline plus cleanup reserve")
    result=supervisor.execute(command,timeout,environment)
    assert result["cleanup"]["group_gone"] and result["cleanup"]["leader_joined"]
    return result

def save(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def expected_metadata_hash(profile, identity):
    metadata={"group_id":"","subject":"","content_type":"","properties":None}
    if profile=="escaped":
        metadata={"group_id":identity+"-group","subject":'receive "qualification"\x00',"content_type":"application/octet-stream","properties":{"escaped":"\x00"*800,"quote":'"\\\n<>&'}}
    raw=json.dumps(metadata,separators=(",",":"),ensure_ascii=False)
    raw=raw.replace("<","\\u003c").replace(">","\\u003e").replace("&","\\u0026")
    return hashlib.sha256(raw.encode()).hexdigest()


def audit_stage(result, ledger, stage):
    assert result["passed"] and result["close_joined"] and ledger["pass"]
    resources = ledger["resources"]
    assert len(resources) == 4 and len({resource["name"] for resource in resources}) == 4
    assert all(resource["create_acknowledged"] and resource["deletion_confirmed"] and resource["confirmed_absent"] for resource in resources)
    assert all(row["empty"] and row["deleted"] for row in result["rows"])
    identities = ledger["identities"]
    assert len(ledger["rpc_trace"]) == len(identities)
    assert len(identities) == sum(row["message_count"] for row in result["rows"])
    assert len({identity["id"] for identity in identities}) == len(identities)
    assert len({(identity["queue"], identity["sequence"]) for identity in identities}) == len(identities)
    owned = {resource["name"] for resource in resources}
    for identity in identities:
        assert identity["queue"] in owned and identity["sequence"] > 0
        assert identity["acknowledged"] and identity["completed"] and identity["deliveries"] == 1
        body = (identity["id"].encode() + b":" + b"p" * 256)[:256]
        assert hashlib.sha256(body).hexdigest() == identity["body_sha256"]
        assert identity["metadata_profile"] in ("plain","escaped")
        assert expected_metadata_hash(identity["metadata_profile"],identity["id"]) == identity["metadata_sha256"]
        acknowledgements = [event for event in ledger["rpc_trace"] if event["queue"] == identity["queue"] and event["sequence"] == identity["sequence"]]
        assert len(acknowledgements) == 1
        assert acknowledgements[0]["operation"] == "CompleteBatch" and acknowledgements[0]["confirmed"] and acknowledgements[0]["http_status"] == 200
    for row in result["rows"]:
        matches = [name for name in owned if name.endswith("-" + row["client"] + "-" + str(row["batch"]))]
        assert len(matches) == 1
        members=[identity for identity in identities if identity["queue"]==matches[0] and identity["metadata_profile"]==row["profile"]]
        assert row["cycles"]>0 and row["message_count"]==row["batch"]*row["cycles"]
        assert len(members)==row["message_count"]
        digest=hashlib.sha256()
        for identity in members:
            digest.update((identity["id"]+":"+str(identity["sequence"])+":"+identity["body_sha256"]+":"+identity["metadata_sha256"]+"\n").encode())
        assert digest.hexdigest()==row["identity_sequence_body_sha256"]
        counts = result["rpc_calls"][matches[0]]
        cycles=sum(item["cycles"] for item in result["rows"] if item["client"]==row["client"] and item["batch"]==row["batch"])
        assert all(counts[operation] == cycles for operation in ("Send", "Receive", "CompleteBatch"))
    samples = result["resource_samples"]
    assert samples and samples[0]["elapsed_seconds"] < 1
    assert samples[-1]["confirmed_identities"] == len(identities)
    for before, after in zip(samples, samples[1:]):
        assert 0 <= after["elapsed_seconds"] - before["elapsed_seconds"] <= 30
        assert after["confirmed_identities"] >= before["confirmed_identities"]
    assert result["heap_growth"] <= 16 << 20
    assert result["goroutine_growth"] <= 16
    if stage == "sustained":
        assert samples[-1]["elapsed_seconds"] >= 180


def audit_provider(provider, outlet=False, local=False):
    snapshot=provider if outlet else provider["snapshot"]
    assert provider["grace_completed" if outlet else "passive_grace_completed"]
    assert provider["server_stream_absence_verified"] is False
    if not local:assert provider["passive_grace_seconds"]>=10
    assert snapshot["dropped"] == 0 and not snapshot["events"], "Provider HTTP error observed; inspect sanitized evidence"


def outlet_intents(queue):
    digest=lambda value:hashlib.sha256(value.encode()).hexdigest()
    intents=[]
    for index in range(3):
        suffix=queue+"-"+str(index)
        values={"message_id":suffix+'\x00id"\u65e5\u672c\u8a9e',"group_id":"\x00group-"+suffix,"correlation_id":"correlation-"+suffix+"\x00tail","reply_to":'reply-"'+suffix+"\x00","subject":"subject\x00"+suffix+"\u65e5\u672c\u8a9e","content_type":"type/"+suffix+"\x00end","state":"","dead_letter_reason":"","dead_letter_description":""}
        intent={field:digest(value) for field,value in values.items()}
        intent["body"]=hashlib.sha256(bytes([0,1,255,2])*64).hexdigest()
        intent["properties"]=digest(json.dumps({"nul":"\x00"*800,"quoted":'"\\\n\u65e5\u672c\u8a9e'},separators=(",",":"),ensure_ascii=False))
        intents.append(intent)
    return intents


def audit_outlets(result, directory):
    assert result["harness_completed"] and result["owner_joined"] and result["physical_absence_confirmed"] and result["passed"]
    assert 0 <= result["cleanup_elapsed_seconds"] <= 60
    clients={"native","sdk"};outlets={"peek","receive","attempt_replay","deferred","dlq"}
    assert set(result["clients"]) == clients
    resources=result["resources"]
    assert len(resources)==2 and len(set(resources))==2
    assert len(result["physical_rows"])==2 and {row["queue"] for row in result["physical_rows"]}==set(resources)
    assert all(row["query_succeeded"] and row["queue_rows"]==0 and row["message_rows"]==0 for row in result["physical_rows"])
    assert len(result["observations"])==20
    expected_fields={"message_id","group_id","correlation_id","reply_to","subject","content_type","body","properties","state","dead_letter_reason","dead_letter_description"}
    digest=lambda value:hashlib.sha256(value.encode()).hexdigest()
    operations=["preflight","create","send","peek","receive","live-attempt","defer","complete-second","deferred","complete-deferred","receive-third","reject","peek-dlq","final-state"]
    assert result["confirmed_operations"]==[client+"/"+operation for client in ("native","sdk") for operation in operations]
    for client in clients:
        summary=result["clients"][client]
        assert summary["passed"] and set(summary["outlets"])==outlets and all(summary["outlets"].values())
        queue=next(name for name in resources if name.endswith("-"+client))
        assert re.fullmatch("m197-outlets-[0-9a-f]{24}-"+client,queue)
        intents=result["intent_sha256"][client]
        assert intents==outlet_intents(queue)
        send_ack=json.loads((directory/(client+"-send-ack.json")).read_text())
        assert len(send_ack)==3 and send_ack[0]>0 and send_ack[0]<send_ack[1]<send_ack[2]
        retained=result["retained_states"][client]
        assert retained["total"]==1 and retained["dead_lettered"]==1
        assert all(retained[key]==0 for key in ("active","locked","deferred","scheduled"))
        assert len(intents)==3 and all(set(intent)==expected_fields for intent in intents)
        assert len({intent["message_id"] for intent in intents})==3
        observations=[row for row in result["observations"] if row["client"]==client]
        assert {outlet:sum(row["outlet"]==outlet for row in observations) for outlet in ("peek","receive","attempt_replay","deferred","receive-third","dlq")}=={"peek":3,"receive":2,"attempt_replay":2,"deferred":1,"receive-third":1,"dlq":1}
        sequences={}
        receives={row["sequence"]:row for row in observations if row["outlet"]=="receive"}
        for row in observations:
            assert row["passed"] and row["sequence"]>0 and all(row["matches"].values())
            expected,actual=row["expected_sha256"],row["actual_sha256"]
            assert all(re.fullmatch("[0-9a-f]{64}",value) for value in list(expected.values())+list(actual.values()))
            assert all(expected[field]==actual[field] for field in expected)
            intent=next(item for item in intents if item["message_id"]==expected["message_id"])
            index=intents.index(intent)
            assert row["sequence"]==send_ack[index]
            for field in expected_fields-{"state","dead_letter_reason","dead_letter_description"}:assert expected[field]==intent[field]
            state="active" if row["outlet"]=="peek" else "dead_lettered" if row["outlet"]=="dlq" else ""
            assert expected["state"]==digest(state)
            assert expected["dead_letter_reason"]==digest('reason\x00tail"' if row["outlet"]=="dlq" else "")
            assert expected["dead_letter_description"]==digest('detail\x00\u65e5\u672c\u8a9e"' if row["outlet"]=="dlq" else "")
            assert row["delivery_count"]==(0 if row["outlet"]=="peek" else 2 if row["outlet"]=="deferred" else 1)
            prior=sequences.setdefault(expected["message_id"],row["sequence"]);assert prior==row["sequence"]
            if row["outlet"]=="attempt_replay":
                assert set(expected)==expected_fields|{"fence","deadline","enqueued"}
                assert all(row["matches"][key] for key in ("fence_equal","deadline_equal","enqueued_equal"))
                assert set(actual)==set(expected)
                assert all(expected[field]==receives[row["sequence"]]["actual_sha256"][field] for field in ("fence","deadline","enqueued"))
            else:
                assert set(expected)==expected_fields
                if row["outlet"]=="receive":
                    assert set(actual)==expected_fields|{"fence","deadline","enqueued"} and row["matches"]["live_fenced_lease"]
                    assert actual["fence"]!=digest("")
                elif row["outlet"]=="deferred":
                    assert set(actual)==expected_fields|{"fence"} and row["matches"]["fresh_live_fence"]
                    assert actual["fence"]!=receives[row["sequence"]]["actual_sha256"]["fence"] and actual["fence"]!=digest("")
                else:assert set(actual)==expected_fields
        assert len(sequences)==3 and len(set(sequences.values()))==3


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--main-repo", type=Path, required=True)
    parser.add_argument("--sdk-repo", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    output = args.out.resolve()
    output.mkdir(parents=True, exist_ok=False)
    plan = json.loads((HERE / "remote-plan.json").read_text())
    assert plan["order"] == ["A", "B", "B", "A", "A", "B", "B", "A"]
    assert plan["sustained_seconds"] == 180
    assert [plan[key] for key in ("lease_seconds","caller_seconds","open_seconds","scenario_seconds_including_open","cleanup_seconds")] == [60,15,45,240,60]
    assert plan["wall_median_B_over_A_max"] == 1.1 and plan["receive_p95_median_B_over_A_max"] == 1.2
    assert [plan[key] for key in ("warmup_cycles","measured_cycles","payload_bytes")] == [1,2,256]
    assert plan["overall_budget_seconds"] == 1800
    assert plan["escaped_comparative_timing"] == "UNAVAILABLE: baseline truncates NUL TEXT"
    assert plan["profile_order"] == [[variant,"plain"] for variant in plan["order"]]
    for pair in plan["sources"].values():
        assert all(re.fullmatch("[0-9a-f]{40}", pair[key]) for key in ("main", "sdk"))
    save(output / "plan.json", plan)
    harness_before = local.source(HERE / "harness")
    save(output / "harness-source.json", harness_before)
    environment = dict(os.environ, GOTOOLCHAIN="local", GOMAXPROCS="2", GOENV="off", GOWORK="off", GOFLAGS="", PYTHONDONTWRITEBYTECODE="1")
    assert environment.get("MQLITE_TEST_DB_RESET_CONFIRM", "") == ""
    destination = environment.get("MQLITE_TEST_DB", "")
    assert hashlib.sha256(destination.encode()).hexdigest() == plan["destination_sha256"]
    assert environment.get("MQLITE_TEST_DB_AUTH_TOKEN")
    build_environment = {key: value for key, value in environment.items() if not key.startswith(("MQLITE_", "MQLITEC_", "TURSO_", "LIBSQL_"))}
    report = {"passed": False, "source_maps": {}, "commands": [], "profile_legs": [], "sustained": [], "remote": True, "escaped_comparative_timing": plan["escaped_comparative_timing"], "original_comparative_gate": "FAIL preserved; not superseded by scoped PASS"}
    work = None
    started=time.monotonic()
    deadline=started+plan["overall_budget_seconds"]
    previous_alarm=signal.signal(signal.SIGALRM,overall_deadline)
    signal.setitimer(signal.ITIMER_REAL,plan["overall_budget_seconds"]-5)
    execute=lambda command,timeout,environment: bounded_execute(command,timeout,environment,deadline)
    try:
        with tempfile.TemporaryDirectory(prefix="m153-remote-") as directory:
            work = Path(directory)
            prepared = {}
            for variant, pair in plan["sources"].items():
                main_source = work / (variant + "-main")
                sdk_source = work / (variant + "-sdk")
                source_maps = {"main": local.checkout(args.main_repo, pair["main"], main_source), "sdk": local.checkout(args.sdk_repo, pair["sdk"], sdk_source)}
                report["source_maps"][variant] = {"main_sha": pair["main"], "sdk_sha": pair["sdk"], "maps_before": source_maps}
                harness = work / (variant + "-harness")
                shutil.copytree(HERE / "harness", harness)
                go_mod = (harness / "go.mod").read_text()
                go_mod = re.sub(r"(?m)^replace github.com/mqlitehq/mqlite => .+$", "replace github.com/mqlitehq/mqlite => " + str(main_source), go_mod)
                go_mod = re.sub(r"(?m)^replace github.com/mqlitehq/mqlite-go => .+$", "replace github.com/mqlitehq/mqlite-go => " + str(sdk_source), go_mod)
                (harness / "go.mod").write_text(go_mod)
                binary = work / (variant + "-workload")
                previous = Path.cwd()
                try:
                    os.chdir(harness)
                    built = execute(["go", "test", "-c", "-p=2", "-o", str(binary), "."], 120, build_environment)
                finally:
                    os.chdir(previous)
                report["commands"].append({"stage": "build-" + variant, **built})
                assert built["exit"] == 0 and not built["timed_out"]
                for name in ("TestRemoteReceiveQualification","TestM197MetadataOutlets","TestProviderObserverTransparentBoundedAndRedacted","TestProviderObserverScopeSQLAndLargeBody","TestProviderObserverConcurrentBoundAndRestore"):
                    selected=execute([str(binary),"-test.list=^"+name+"$"],30,build_environment)
                    report["commands"].append({"stage":"test-selection-"+variant,"test":name,**selected})
                    assert selected["exit"]==0 and not selected["timed_out"]
                    expected=(name+"\n").encode()
                    assert selected["opaque_output_bytes"] == len(expected) and selected["opaque_output_sha256"]==hashlib.sha256(expected).hexdigest()
                controlled=execute([str(binary),"-test.run=^TestProviderObserver","-test.timeout=30s"],40,build_environment)
                report["commands"].append({"stage":"provider-controls-"+variant,**controlled})
                assert controlled["exit"]==0 and not controlled["timed_out"]
                prepared[variant] = (main_source, sdk_source, harness, binary)
                for control_profile in ("plain","escaped"):
                    control_directory=output/("controls-"+variant+"-"+control_profile)
                    control_env = dict(build_environment, M153_REMOTE_STAGE="controls",M153_REMOTE_PROFILE=control_profile, M153_REMOTE_OUT=str(control_directory / "result.json"))
                    controlled = execute([str(binary), "-test.run=^(TestRemoteReceiveQualification|TestReceiveQualificationRejectsLedgerFaults)$", "-test.timeout=90s"], 100, control_env)
                    report["commands"].append({"stage": "controls-" + variant,"profile":control_profile, **controlled})
                    assert controlled["exit"] == 0 and not controlled["timed_out"]
                    audit_stage(json.loads((control_directory/"result.json").read_text()),json.loads((control_directory/"evidence.json").read_text()),"controls")
                if variant == "B":
                    outlet_directory=output/"controls-B-outlets"
                    outlet_env=dict(build_environment, M197_OUTLETS_MODE="local", M197_OUTLETS_OUT=str(outlet_directory))
                    controlled=execute([str(binary), "-test.run=^TestM197MetadataOutlets$", "-test.timeout=305s"],310,outlet_env)
                    report["commands"].append({"stage":"controls-B-outlets",**controlled})
                    assert controlled["exit"] == 0 and not controlled["timed_out"]
                    audit_outlets(json.loads((outlet_directory/"outlets.json").read_text()),outlet_directory)
                report["source_maps"][variant]["binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()

            def execute_stage(stage, variant, index, profile="plain", sustained_profile="") :
                assert deadline - time.monotonic() >= 310, "Remaining frozen budget cannot contain a full stage and cleanup"
                destination = output / (stage + "-" + str(index) + "-" + variant)
                stage_environment = dict(environment, M153_REMOTE_STAGE=stage,M153_REMOTE_PROFILE=profile, M153_REMOTE_OUT=str(destination / "result.json"),M197_SUSTAINED_PROFILE=sustained_profile)
                command = [str(prepared[variant][3]), "-test.run=^TestRemoteReceiveQualification$", "-test.timeout=305s"]
                executed = execute(command, 310, stage_environment)
                report["commands"].append({"stage": stage, "variant": variant, "index": index, **executed})
                save(output / "progress.json", report)
                assert executed["exit"] == 0 and not executed["timed_out"]
                result = json.loads((destination / "result.json").read_text())
                ledger = json.loads((destination / "evidence.json").read_text())
                audit_stage(result, ledger, stage)
                audit_provider(json.loads((destination/"provider.json").read_text()))
                return result

            for index, (variant,profile) in enumerate(plan["profile_order"]):
                result = execute_stage("profiles", variant, index,profile)
                assert len(result["rows"]) == 4 and all(row["cycles"] == 3 and row["message_count"] == row["batch"] * 3 for row in result["rows"])
                report["profile_legs"].append({"variant": variant,"profile":profile, "data": result})
                save(output / "progress.json", report)
                print(json.dumps({"stage": "profiles", "variant": variant, "index": index, "passed": True}), flush=True)
            timing = {}
            for client in ("native", "sdk"):
                for batch in (1, 8):
                    for metadata_profile in ("plain",):
                        rows = {variant: [row for leg in report["profile_legs"] if leg["variant"] == variant for row in leg["data"]["rows"] if row["client"] == client and row["batch"] == batch and row["profile"]==metadata_profile] for variant in ("A", "B")}
                        profile = {}
                        for metric, limit in (("wall_ms", 1.1), ("receive_p95_ms", 1.2)):
                            before = [row[metric] for row in rows["A"]]
                            after = [row[metric] for row in rows["B"]]
                            ratio = statistics.median(after) / statistics.median(before)
                            profile[metric] = {"A": before, "B": after, "median_ratio_B_over_A": ratio, "limit": limit, "within": ratio <= limit}
                        timing[client + "-" + str(batch)+"-"+metadata_profile] = profile
            report["timing"] = timing
            save(output / "timing.json", timing)
            assert all(metric["within"] for profile in timing.values() for metric in profile.values()), "Frozen remote timing screen failed"
            escaped=execute_stage("profiles","B","escaped-strict","escaped")
            assert len(escaped["rows"]) == 4 and all(row["cycles"]==3 for row in escaped["rows"])
            report["B_escaped_strict"]={"data":escaped,"comparative_timing":"UNAVAILABLE"}
            save(output/"progress.json",report)
            assert deadline-time.monotonic() >= 310
            outlet_directory=output/"outlets-B"
            outlet_environment=dict(environment,M197_OUTLETS_MODE="remote",M197_OUTLETS_OUT=str(outlet_directory))
            executed=execute([str(prepared["B"][3]),"-test.run=^TestM197MetadataOutlets$","-test.timeout=305s"],310,outlet_environment)
            report["commands"].append({"stage":"outlets","variant":"B",**executed})
            save(output/"progress.json",report)
            assert executed["exit"]==0 and not executed["timed_out"]
            outlet=json.loads((outlet_directory/"outlets.json").read_text())
            audit_outlets(outlet,outlet_directory)
            audit_provider(json.loads((outlet_directory/"provider.json").read_text()),outlet=True)
            report["B_outlets"]=outlet
            for index, (variant,metadata_scope) in enumerate((("A","plain"),("B","mixed"))):
                result = execute_stage("sustained", variant, index, sustained_profile=metadata_scope)
                assert result["elapsed_seconds"] >= 180 and len(result["rows"]) == (2 if metadata_scope=="plain" else 4)
                report["sustained"].append({"variant": variant, "metadata_scope":metadata_scope,"comparison":"independent stability; different work, no throughput ratio", "data": result})
                print(json.dumps({"stage": "sustained", "variant": variant, "passed": True}), flush=True)
            for variant, (main_source, sdk_source, harness, binary) in prepared.items():
                after = {"main": local.source(main_source), "sdk": local.source(sdk_source)}
                unchanged = after == report["source_maps"][variant]["maps_before"]
                report["source_maps"][variant]["maps_after_equal"] = unchanged
                assert unchanged
            report["passed"] = True
    except BaseException as error:
        report["passed"] = False
        report["failure_type"] = type(error).__name__
        report["failure_reason"] = "Inspect retained sanitized stage and source evidence"
    finally:
        signal.setitimer(signal.ITIMER_REAL,0)
        signal.signal(signal.SIGALRM,previous_alarm)
        report["overall_budget_seconds"]=plan["overall_budget_seconds"]
        report["cleanup_reserve_seconds"]=5
        report["controller_elapsed_seconds"]=time.monotonic()-started
        report["at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        report["source_directories_removed"] = work is not None and not work.exists()
        report["harness_unchanged"] = local.source(HERE / "harness") == harness_before
        report["scope"] = "Final source common-valid plain A/B timing; B strict escaped and metadata outlets; Aplain/Bmixed independent180s stability. Escaped comparative timing unavailable; original gate FAIL preserved. Physical integrity and 48h final acceptance separate."
        report["passed"] = report["passed"] and report["source_directories_removed"] and report["harness_unchanged"] and report["controller_elapsed_seconds"]<=1800
        save(output / "result.json", report)
    return 0 if report["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
