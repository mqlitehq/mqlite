#!/usr/bin/env python3
"""Exercise the isolated Compose stack through real broker, Prometheus and Grafana APIs.

Only obsdemo-* queues and this directory's Compose project are changed. Credentials
are read from files and never printed. --faults temporarily interrupts this stack.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import secrets
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request

ROOT = Path(__file__).resolve().parents[2]
STACK = ROOT / "ops" / "observability"
BROKER = "http://127.0.0.1:" + os.environ.get("MQLITE_OBS_PORT", "17654")
PROM = "http://127.0.0.1:" + os.environ.get("PROMETHEUS_OBS_PORT", "19190")
GRAFANA = "http://127.0.0.1:" + os.environ.get("GRAFANA_OBS_PORT", "13000")
ADMIN = ""
MONITOR = ""
GRAFANA_AUTH = ""
DATA = None
CHECKS = []
OBS = "/mqlite.v1.AdminService/Observe"


def request(url, data=None, auth="", method=None):
    payload = None if data is None else json.dumps(data).encode()
    req = urllib.request.Request(url, data=payload, method=method)
    if payload is not None:
        req.add_header("Content-Type", "application/json")
    if auth:
        req.add_header("Authorization", auth)
    try:
        response = urllib.request.urlopen(req, timeout=25)
    except urllib.error.HTTPError as error:
        response = error
    with response:
        raw = response.read()
        try:
            body = json.loads(raw)
        except (json.JSONDecodeError, UnicodeDecodeError):
            body = raw.decode(errors="replace")
        return response.status, body


def check(condition, label):
    if not condition:
        raise AssertionError(label)
    CHECKS.append(label)
    print("PASS " + label, flush=True)


def wait_for(label, predicate, timeout=90):
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                check(True, label)
                return
        except (OSError, ValueError, KeyError, AssertionError) as error:
            last = type(error).__name__
        time.sleep(1)
    raise AssertionError(label + " timed out" + (" (" + last + ")" if last else ""))


def rpc(service, method, data, token=None, expected=200):
    token = ADMIN if token is None else token
    status, result = request(BROKER + "/mqlite.v1." + service + "/" + method,
                             data, "Bearer " + token if token else "")
    if status != expected:
        raise AssertionError(f"{service}/{method}: HTTP {status}, expected {expected}")
    return result


def query(expr):
    status, data = request(PROM + "/api/v1/query?" + urllib.parse.urlencode({"query": expr}))
    if status != 200 or data.get("status") != "success":
        raise AssertionError("Prometheus query failed")
    return data["data"]["result"]


def value(expr):
    rows = query(expr)
    return float(rows[0]["value"][1]) if len(rows) == 1 else None


def queue(name, config=None):
    assert name.startswith("obsdemo-")
    rpc("AdminService", "CreateQueue", {"name": name, "config": config or {}})
    rpc("AdminService", "Purge", {"queue": name, "max": 10000})
    # Purge intentionally targets only the DLQ. Reset our own scheduled/deferred
    # demo work through its real public operations, then drain active messages.
    for state in ("scheduled", "deferred"):
        while True:
            rows = rpc("QueueService", "Peek", {"queue": name, "state": state, "max": 100})["messages"]
            if not rows:
                break
            for row in rows:
                if state == "scheduled":
                    rpc("QueueService", "Cancel", {"queue": name, "seq_number": row["seq_number"]})
                else:
                    result = rpc("QueueService", "ReceiveDeferred", {"queue": name, "seq_numbers": [row["seq_number"]]})
                    for message in result["messages"]:
                        settle("Complete", name, message)
    while receive(name, maximum=100, receive_mode=1):
        pass
    remaining = rpc("QueueService", "Stats", {"queue": name})
    if remaining["locked"]:
        raise AssertionError(name + " has a previous live lease; wait for expiry and retry")


def send(name, text, **extra):
    message = {"body": base64.b64encode(text.encode()).decode()}
    return rpc("QueueService", "Send", {"queue": name, "messages": [message], **extra})


def receive(name, maximum=1, **extra):
    return rpc("QueueService", "Receive", {"queue": name, "max_messages": maximum, **extra})["messages"]


def settle(method, name, message, **extra):
    return rpc("QueueService", method, {"queue": name, "seq_number": message["seq_number"],
                                        "lock_token": message["lock_token"], **extra})


def event(name, kind):
    observation = rpc("AdminService", "Observe", {}, token=MONITOR)
    return next((entry["count"] for entry in observation["messages"]
                 if entry["queue"] == name and entry["event"] == kind), 0)


def grafana_query(expr, datasource="mqlite-prometheus", query_format="time_series"):
    now = int(time.time() * 1000)
    return request(GRAFANA + "/api/ds/query", {
        "from": str(now - 60000), "to": str(now),
        "queries": [{"refId": "A", "datasource": {"type": "prometheus", "uid": datasource},
                     "expr": expr, "instant": True, "range": False, "format": query_format,
                     "intervalMs": 5000, "maxDataPoints": 100}],
    }, GRAFANA_AUTH)


def grafana_result(status, data):
    result = data.get("results", {}).get("A") if isinstance(data, dict) else None
    if status != 200 or not isinstance(result, dict) or result.get("error") \
            or result.get("status", 200) != 200 or not isinstance(result.get("frames"), list):
        raise AssertionError("Grafana datasource did not return a successful query result")
    return result


def grafana_ok():
    try:
        result = grafana_result(*grafana_query('up{job="mqlite"}'))
        return bool(result["frames"])
    except AssertionError:
        return False


def rejected(call, label):
    try:
        call()
    except AssertionError:
        check(True, label)
        return
    raise AssertionError("negative control was incorrectly accepted: " + label)


def alert_firing(name):
    status, data = request(PROM + "/api/v1/alerts")
    assert status == 200
    return any(a["labels"].get("alertname") == name and a["state"] == "firing"
               for a in data["data"]["alerts"])


def panel_alert_firing(name, selected_queue):
    board = json.loads((STACK / "grafana/dashboards/mqlite.json").read_text())
    panel = next(panel for panel in board["panels"] if panel["id"] == 16)
    expr = panel["targets"][0]["expr"].replace("$instance", ".*").replace("$queue", selected_queue)
    return any(row["metric"].get("alertname") == name for row in query(expr))


def verify():
    wait_for("broker liveness", lambda: request(BROKER + "/healthz")[0] == 200)
    wait_for("Prometheus ready", lambda: request(PROM + "/-/ready")[0] == 200)
    wait_for("Grafana ready", lambda: request(GRAFANA + "/api/health")[0] == 200)
    wait_for("actual authenticated scrape succeeds", lambda: value('up{job="mqlite"}') == 1)
    wait_for("canonical snapshot collection succeeds", lambda: value("mqlite_collection_success") == 1)
    check(ADMIN != MONITOR, "monitor and administrator credentials differ")
    for token, expected in (("", 401), ("invalid-demo-credential", 401), (MONITOR, 200)):
        status, _ = request(BROKER + "/metrics", auth="Bearer " + token if token else "")
        check(status == expected, "metrics authentication HTTP " + str(expected))
    observe = rpc("AdminService", "Observe", {}, token=MONITOR)
    check(observe["access"] == "monitor" and observe["collection"]["state"] == "available",
          "monitor reads canonical observation")
    for service, method, body in (
        ("QueueService", "Send", {"queue": "obsdemo-denied", "messages": []}),
        ("QueueService", "Receive", {"queue": "obsdemo-denied"}),
        ("QueueService", "Peek", {"queue": "obsdemo-denied"}),
        ("AdminService", "CreateQueue", {"name": "obsdemo-denied"}),
        ("AuthService", "CreateKey", {"id": secrets.token_hex(16), "name": "denied", "permissions": ["manage"]}),
    ):
        rpc(service, method, body, token=MONITOR, expected=403)
        check(True, "monitor cannot " + service + "/" + method)

    escaped = 'obsdemo-label-"-\\-\t-\n'
    queue(escaped)
    send(escaped, "label escaping")
    wait_for("Prometheus ingests quote, backslash, tab and newline queue labels",
             lambda: value("mqlite_queue_retained_messages{queue=" + json.dumps(escaped) + "}") == 1)
    check(any(item["queue"] == escaped for item in rpc("AdminService", "Observe", {}, token=MONITOR)["queues"]),
          "native observation preserves the same control-character queue name")

    q = "obsdemo-flow"
    queue(q)
    before = event(q, "completed")
    body = "observability identity " + secrets.token_hex(8)
    seq = send(q, body)["seq_numbers"][0]
    messages = receive(q)
    check(len(messages) == 1 and messages[0]["seq_number"] == seq
          and base64.b64decode(messages[0]["body"]).decode() == body,
          "actual sent identity and full body survive delivery")
    settle("Complete", q, messages[0])
    settle("Complete", q, messages[0])
    check(event(q, "completed") == before + 1, "exact settlement replay adds one completion only")
    wait_for("Prometheus observes canonical completion counter",
             lambda: value(f'mqlite_message_events_total{{queue="{q}",event="completed"}}') == before + 1)
    check(not receive(q, wait_time_ms=100), "empty long poll is a successful empty result")

    queue("obsdemo-backlog")
    for index in range(3):
        send("obsdemo-backlog", "waiting " + str(index))
    wait_for("consumer stop produces retained backlog",
             lambda: value('mqlite_queue_messages{queue="obsdemo-backlog",state="active"}') == 3)
    wait_for("oldest outstanding age is visible",
             lambda: (value('mqlite_queue_oldest_message_age_seconds{queue="obsdemo-backlog"}') or 0) > 0)

    queue("obsdemo-retry")
    send("obsdemo-retry", "retry once")
    first = receive("obsdemo-retry")[0]
    settle("Abandon", "obsdemo-retry", first)
    second = receive("obsdemo-retry")[0]
    check(second["seq_number"] == first["seq_number"] and second["delivery_count"] == first["delivery_count"] + 1,
          "abandon produces a real redelivery")
    settle("Complete", "obsdemo-retry", second)
    check(event("obsdemo-retry", "redelivered") >= 1, "canonical redelivery is recorded")

    queue("obsdemo-deferred")
    send("obsdemo-deferred", "deferred work")
    held = receive("obsdemo-deferred")[0]
    settle("Defer", "obsdemo-deferred", held)
    queue("obsdemo-scheduled")
    rpc("QueueService", "Schedule", {"queue": "obsdemo-scheduled", "messages": [{"body": "c2NoZWR1bGVk"}],
                                       "scheduled_enqueue_time_ms": int(time.time() * 1000) + 3600000})
    wait_for("deferred and scheduled states appear separately",
             lambda: value('mqlite_queue_messages{queue="obsdemo-deferred",state="deferred"}') == 1
             and value('mqlite_queue_messages{queue="obsdemo-scheduled",state="scheduled"}') == 1)

    queue("obsdemo-alert")
    send("obsdemo-alert", "demonstration rejection")
    bad = receive("obsdemo-alert")[0]
    settle("Reject", "obsdemo-alert", bad, dead_letter_reason="demo-rejection")
    wait_for("real Prometheus demo rule fires", lambda: alert_firing("MQLiteDemoDeadLetter"))
    wait_for("alert panel includes the selected queue's firing alert",
             lambda: panel_alert_firing("MQLiteDemoDeadLetter", "obsdemo-alert"))
    check(not panel_alert_firing("MQLiteDemoDeadLetter", "obsdemo-backlog"),
          "alert panel excludes other queues' firing alerts")
    rpc("AdminService", "Redrive", {"queue": "obsdemo-alert", "max": 1})
    settle("Complete", "obsdemo-alert", receive("obsdemo-alert")[0])
    wait_for("real Prometheus demo rule resolves", lambda: not alert_firing("MQLiteDemoDeadLetter"))
    wait_for("resolved alert disappears from the selected queue panel",
             lambda: not panel_alert_firing("MQLiteDemoDeadLetter", "obsdemo-alert"))

    # Keep one explicitly named demonstration DLQ for the operator to inspect.
    queue("obsdemo-dlq")
    send("obsdemo-dlq", "intentional retained dead letter")
    settle("Reject", "obsdemo-dlq", receive("obsdemo-dlq")[0], dead_letter_reason="demo-inspect")

    queue("obsdemo-ttl-discard", {"dead_letter_on_expire": False})
    discarded = event("obsdemo-ttl-discard", "ttl_discarded")
    send("obsdemo-ttl-discard", "expires without dead lettering", ttl_ms=500)
    wait_for("actual TTL maintenance records discard",
             lambda: event("obsdemo-ttl-discard", "ttl_discarded") == discarded + 1)
    check(rpc("QueueService", "Stats", {"queue": "obsdemo-ttl-discard"})["total"] == 0,
          "discarded TTL message is no longer retained")
    queue("obsdemo-ttl-dlq", {"dead_letter_on_expire": True})
    dead_lettered = event("obsdemo-ttl-dlq", "dead_lettered")
    send("obsdemo-ttl-dlq", "expires into dead letters", ttl_ms=500)
    wait_for("actual TTL maintenance creates a dead letter",
             lambda: rpc("QueueService", "Stats", {"queue": "obsdemo-ttl-dlq"})["dead_lettered"] == 1)
    wait_for("Prometheus observes the TTL dead-letter transition",
             lambda: value('mqlite_message_events_total{queue="obsdemo-ttl-dlq",event="dead_lettered"}') == dead_lettered + 1)

    queue("obsdemo-retention", {"dlq_max_count": 1})
    retained = event("obsdemo-retention", "retention_deleted")
    for index in range(3):
        send("obsdemo-retention", "retention " + str(index))
        settle("Reject", "obsdemo-retention", receive("obsdemo-retention")[0], dead_letter_reason="demo-retention")
    wait_for("real retention janitor deletes excess dead letters",
             lambda: event("obsdemo-retention", "retention_deleted") == retained + 2, timeout=100)
    check(rpc("QueueService", "Stats", {"queue": "obsdemo-retention"})["dead_lettered"] == 1,
          "retention bound leaves the configured one dead letter")
    wait_for("Prometheus observes retention deletion effects",
             lambda: value('mqlite_message_events_total{queue="obsdemo-retention",event="retention_deleted"}') == retained + 2)

    invalid_filter_query = 'mqlite_rpc_requests_total{rpc="AdminService/Subscribe",code="invalid_argument"}'
    invalid_filters = value(invalid_filter_query) or 0
    rpc("AdminService", "Subscribe", {"topic": "obsdemo-topic", "name": "obsdemo-invalid-filter",
                                      "filter": {"expr": "("}}, expected=400)
    wait_for("invalid filter configuration is an RPC validation error",
             lambda: (value(invalid_filter_query) or 0) >= invalid_filters + 1)
    rpc("AdminService", "Subscribe", {"topic": "obsdemo-topic", "name": "obsdemo-filter-runtime",
                                      "filter": {"expr": "int(body_text) > 0"}})
    evaluation_failures = next(item["count"] for item in
                              rpc("AdminService", "Observe", {}, token=MONITOR)["filters"]
                              if item["stage"] == "evaluate")
    send("obsdemo-topic", "not-a-number")
    wait_for("Prometheus observes a real routing filter evaluation failure",
             lambda: value('mqlite_filter_failures_total{stage="evaluate"}') == evaluation_failures + 1)

    key = rpc("AuthService", "CreateKey", {"id": secrets.token_hex(16), "name": "observability-expiry-test",
              "permissions": ["send"], "expires_at_ms": int(time.time() * 1000) + 1500})
    rpc("QueueService", "Receive", {"queue": q}, token=key["token"], expected=403)
    wait_for("expired managed key is rejected", lambda: request(BROKER + "/mqlite.v1.QueueService/Send",
             {"queue": q, "messages": []}, "Bearer " + key["token"])[0] == 401)
    revoked = rpc("AuthService", "CreateKey", {"id": secrets.token_hex(16), "name": "observability-revoked-test", "permissions": ["listen"]})
    rpc("AuthService", "RevokeKey", {"id": revoked["key"]["id"]})
    rpc("QueueService", "Receive", {"queue": q}, token=revoked["token"], expected=401)
    check(True, "revoked managed key is rejected")
    wait_for("Prometheus sees bounded authentication outcomes",
             lambda: all((value(f'mqlite_authentication_total{{outcome="{outcome}"}}') or 0) > 0
                         for outcome in ("missing", "invalid", "expired", "revoked", "permission_denied")))

    snapshot = rpc("AdminService", "Observe", {}, token=MONITOR)
    queues = {item["queue"]: item for item in snapshot["queues"]}
    for name in ("obsdemo-backlog", "obsdemo-deferred", "obsdemo-scheduled"):
        check(value(f'mqlite_queue_retained_messages{{queue="{name}"}}') == queues[name]["total"],
              "native and Prometheus retained values agree: " + name)
        check(value(f'mqlite_queue_total{{queue="{name}"}}') == queues[name]["total"],
              "legacy total alias uses same value: " + name)
    status, targets = request(PROM + "/api/v1/targets")
    check(status == 200 and any(t["labels"].get("job") == "mqlite" and t["health"] == "up" and not t["lastError"]
                               for t in targets["data"]["activeTargets"]), "Prometheus target has no scrape error")
    status, board = request(GRAFANA + "/api/dashboards/uid/mqlite-observability", auth=GRAFANA_AUTH)
    check(status == 200 and board["meta"]["provisioned"] and len(board["dashboard"]["panels"]) >= 20,
          "Grafana provisioned standard dashboard")
    wait_for("Grafana datasource performs a real Prometheus query", grafana_ok)
    rejected(lambda: query("("), "verifier rejects a real invalid Prometheus expression")
    rejected(lambda: grafana_result(*grafana_query("(")),
             "verifier rejects a real invalid Grafana panel expression")
    rejected(lambda: grafana_result(*grafana_query("up", datasource="missing-demo-datasource")),
             "verifier rejects a missing Grafana datasource")
    rejected(lambda: grafana_result(200, {"results": {}}),
             "verifier rejects a success envelope with no query result")
    # Query every panel through Grafana after expanding the three dashboard variables.
    for panel in board["dashboard"]["panels"]:
        for target in panel.get("targets", []):
            expr = target["expr"].replace("$instance", ".*").replace("$queue", ".*").replace("$__rate_interval", "1m")
            grafana_result(*grafana_query(expr, query_format=target.get("format", "time_series")))
            check(True, "Grafana panel query: " + panel["title"])


def compose(*args):
    subprocess.run(["docker", "compose", *args], cwd=STACK, check=True,
                   stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, timeout=90)


def faults():
    token_file = DATA / "secrets/monitor.token"
    original = token_file.read_bytes()
    try:
        # In-place write preserves the inode of the Compose secret bind mount.
        token_file.write_text("invalid-observability-demo-token\n")
        wait_for("invalid scraper credential makes target down", lambda: value('up{job="mqlite"}') == 0)
        wait_for("production scrape-failure rule fires", lambda: alert_firing("MQLiteScrapeUnavailable"), timeout=130)
        wait_for("queue selection retains broker-wide firing alerts without a queue label",
                 lambda: panel_alert_firing("MQLiteScrapeUnavailable", "obsdemo-alert"))
    finally:
        token_file.write_bytes(original)
        token_file.chmod(0o600)
    wait_for("credential restoration recovers scrape", lambda: value('up{job="mqlite"}') == 1)
    wait_for("production scrape-failure rule resolves", lambda: not alert_firing("MQLiteScrapeUnavailable"))
    wait_for("resolved broker-wide alert disappears despite the queue selection",
             lambda: not panel_alert_firing("MQLiteScrapeUnavailable", "obsdemo-alert"))

    try:
        compose("stop", "prometheus")
        wait_for("Grafana reports datasource failure rather than healthy zero", lambda: not grafana_ok())
    finally:
        compose("start", "prometheus")
    wait_for("Prometheus recovers", lambda: request(PROM + "/-/ready")[0] == 200)
    wait_for("Grafana datasource recovers", grafana_ok)

    started = value("mqlite_start_time_seconds")
    compose("restart", "mqlite")
    wait_for("broker restart changes process start time", lambda: (value("mqlite_start_time_seconds") or 0) > started)
    check(rpc("QueueService", "Stats", {"queue": "obsdemo-backlog"})["active"] == 3,
          "broker restart preserves SQLite backlog")
    check(event("obsdemo-flow", "completed") == 0, "process-local completion counter resets at restart")


def traffic(seconds):
    import fcntl
    import signal
    import sys
    import threading

    if seconds <= 0:
        raise ValueError("traffic duration must be positive")
    name = "obsdemo-live"
    stop = threading.Event()
    pending = {}
    held = {}
    totals = {key: 0 for key in ("rounds", "enqueued", "completed", "abandoned", "rejected", "redriven", "peak_retained")}

    def stopping():
        return stop.is_set() or time.monotonic() >= deadline

    def pause(duration):
        stop.wait(max(0, min(duration, deadline - time.monotonic())))

    def state(phase):
        stats = rpc("QueueService", "Stats", {"queue": name})
        if stats["total"] > 48:
            raise AssertionError("live traffic exceeded its 48-message bound")
        totals["peak_retained"] = max(totals["peak_retained"], stats["total"])
        print(f"TRAFFIC round={totals['rounds'] + 1} phase={phase} "
              f"active={stats['active']} locked={stats['locked']} dlq={stats['dead_lettered']} "
              f"retained={stats['total']}", flush=True)
        return stats

    def publish(count, kind):
        bodies = [f"observability live {kind} {secrets.token_hex(16)}" for _ in range(count)]
        result = rpc("QueueService", "Send", {"queue": name, "messages": [
            {"body": base64.b64encode(body.encode()).decode()} for body in bodies]})
        seqs = result["seq_numbers"]
        if len(seqs) != count or len(set(seqs)) != count or any(seq <= 0 or seq in pending for seq in seqs):
            raise AssertionError("live send did not acknowledge distinct message identities")
        pending.update(zip(seqs, bodies))
        totals["enqueued"] += count

    def claim(maximum):
        rows = receive(name, maximum=maximum)
        if not rows or len(rows) > maximum:
            raise AssertionError("live receive did not return the expected work")
        held.update((row["seq_number"], row) for row in rows)
        for row in rows:
            seq = row["seq_number"]
            if seq not in pending or base64.b64decode(row["body"]).decode() != pending[seq]:
                raise AssertionError("live delivery identity or complete body mismatch")
        return rows

    def finish(method, row, **extra):
        settle(method, name, row, **extra)
        held.pop(row["seq_number"], None)
        if method == "Complete":
            pending.pop(row["seq_number"])
            totals["completed"] += 1

    # One driver per initialized stack. The kernel releases this lock after a
    # crash; a second process cannot reset a live driver's queue.
    with (DATA / "traffic.lock").open("a") as lock:
        os.chmod(lock.name, 0o600)
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise AssertionError("another traffic driver owns this stack; stop it before starting another") from None

        status, prior = request(BROKER + "/mqlite.v1.QueueService/Stats", {"queue": name}, "Bearer " + ADMIN)
        if status == 200:
            if prior["locked"]:
                raise AssertionError("obsdemo-live has a live lease; wait for expiry before restarting traffic")
            if prior["total"] > 96 or prior["total"] != prior["active"] + prior["dead_lettered"]:
                raise AssertionError("obsdemo-live contains unexpected work; inspect it before starting traffic")
        elif status != 404:
            raise AssertionError("cannot inspect the live demo queue")
        rpc("AdminService", "CreateQueue", {"name": name, "config": {
            "lock_duration_ms": 5000, "default_ttl_ms": 120000, "dead_letter_on_expire": False,
            "dlq_max_count": 48, "dlq_max_age_ms": 120000}})
        # Only bounded leftovers from an interrupted run of this reserved queue.
        rpc("AdminService", "Purge", {"queue": name, "max": 96})
        receive(name, maximum=96, receive_mode=1)
        if rpc("QueueService", "Stats", {"queue": name})["total"]:
            raise AssertionError("live queue did not reset; another client may be using it")

        started = time.monotonic()
        deadline = started + seconds
        previous_handler = signal.signal(signal.SIGINT, lambda *_: stop.set())
        print(f"TRAFFIC requested_seconds={seconds} queue={name}; Ctrl-C stops new work and drains this round.", flush=True)
        try:
            while not stopping():
                size = 24 if totals["rounds"] % 2 == 0 else 48
                produce_rate, consume_rate = size // 8, size // 4
                for _ in range(8):
                    if stopping():
                        break
                    publish(produce_rate, "backlog")
                    pause(1)
                state("backlog")
                pause(10)
                while pending:
                    for row in claim(min(consume_rate, len(pending))):
                        finish("Complete", row)
                    pause(1)
                state("recovered")

                for _ in range(3):
                    if stopping():
                        break
                    publish(1, "retry")
                    first = claim(1)[0]
                    finish("Abandon", first)
                    totals["abandoned"] += 1
                    pause(1)
                    second = claim(1)[0]
                    if second["seq_number"] != first["seq_number"] or second["delivery_count"] != first["delivery_count"] + 1:
                        raise AssertionError("abandon did not produce a real redelivery")
                    finish("Complete", second)

                if not stopping():
                    publish(3, "dead-letter")
                    for row in claim(3):
                        finish("Reject", row, dead_letter_reason="observability-live-demo")
                        totals["rejected"] += 1
                    if state("dead-letter")["dead_lettered"] != 3:
                        raise AssertionError("live rejection did not retain three dead letters")
                    pause(10)
                    moved = rpc("AdminService", "Redrive", {"queue": name, "max": 3})["moved"]
                    if moved != 3:
                        raise AssertionError("live redrive did not restore all three identities")
                    totals["redriven"] += moved
                    while pending:
                        for row in claim(len(pending)):
                            finish("Complete", row)
                if state("drained")["total"] != 0 or held:
                    raise AssertionError("live round left retained messages or unsettled deliveries")
                totals["rounds"] += 1
                pause(5)
        finally:
            # Unexpected failures release known leases; TTL/retention bounds the
            # small remainder. Do not silently purge evidence after a failure.
            failed = sys.exc_info()[0] is not None
            cleanup_failures = 0
            try:
                for row in list(held.values()):
                    try:
                        settle("Abandon", name, row)
                    except Exception:
                        cleanup_failures += 1
            finally:
                signal.signal(signal.SIGINT, previous_handler)
            if cleanup_failures:
                print(f"WARN could not abandon {cleanup_failures} held deliveries; expiry/TTL still applies.", file=sys.stderr)
                if not failed:
                    raise AssertionError("live traffic lease cleanup failed")

    result = {"status": "PASS", "mode": "traffic", "queue": name, **totals,
              "requested_seconds": seconds, "elapsed_seconds": round(time.monotonic() - started, 3),
              "stopped_by": "interrupt" if stop.is_set() else "duration"}
    print("PASS live traffic " + json.dumps(result, sort_keys=True), flush=True)
    return result


def main():
    global ADMIN, MONITOR, GRAFANA_AUTH, DATA
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--faults", action="store_true", help="also interrupt this isolated scraper/broker and verify recovery")
    parser.add_argument("--traffic-only", action="store_true", help="run live traffic without verification or fault drills")
    parser.add_argument("--traffic-seconds", type=int, default=0, help="generate bounded wave traffic for this duration, then drain in-flight work")
    parser.add_argument("--output", type=Path, help="write non-secret JSON evidence outside the repository")
    args = parser.parse_args()
    if args.traffic_seconds < 0 or (args.traffic_only and args.traffic_seconds == 0):
        parser.error("traffic duration must be positive when --traffic-only is used")
    if args.traffic_only and args.faults:
        parser.error("--traffic-only cannot be combined with --faults")
    config = dict(line.split("=", 1) for line in (STACK / ".env").read_text().splitlines()
                  if line and not line.startswith("#") and "=" in line)
    DATA = Path(os.environ.get("OBS_DATA_DIR", config["OBS_DATA_DIR"].strip('"')))
    ADMIN = (DATA / "secrets/admin.token").read_text().strip()
    MONITOR = (DATA / "secrets/monitor.token").read_text().strip()
    password = (DATA / "secrets/grafana-admin.token").read_text().strip()
    GRAFANA_AUTH = "Basic " + base64.b64encode(("admin:" + password).encode()).decode()
    started = time.time()

    def output(report):
        if args.output:
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(report, indent=2) + "\n")

    try:
        output({"status": "RUNNING", "mode": "traffic" if args.traffic_only else "verification"})
        if args.traffic_only:
            output(traffic(args.traffic_seconds))
            return
        verify()
        if args.faults:
            faults()
        report = {"status": "PASS", "checks": CHECKS, "faults_tested": args.faults,
                  "broker": BROKER, "prometheus": PROM, "grafana": GRAFANA}
        if args.traffic_seconds > 0:
            report["traffic"] = traffic(args.traffic_seconds)
        report["elapsed_seconds"] = round(time.time() - started, 3)
        output(report)
        print(f"PASS {len(CHECKS)} checks", flush=True)
    except Exception as error:
        text = str(error)
        for credential in (ADMIN, MONITOR, password, GRAFANA_AUTH):
            text = text.replace(credential, "[redacted]")
        output({"status": "FAIL", "checks": CHECKS, "error": type(error).__name__ + ": " + text,
                "elapsed_seconds": round(time.time() - started, 3)})
        raise SystemExit(type(error).__name__ + ": " + text) from None


if __name__ == "__main__":
    main()
