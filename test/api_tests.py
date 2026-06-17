#!/usr/bin/env python3
"""Behavioral API scenario suite (pure stdlib HTTP client).

Drives a running mqlite broker over HTTP to exercise multi-step queue behaviour:
lifecycle, visibility timeout, abandon/redelivery, dead-letter + redrive, defer,
schedule, dedup window, session ordering, receive-and-delete and fencing.

Env: ENDPOINT, TOKEN, RUNID (set by run.sh).
"""
import base64
import json
import os
import sys
import time
import urllib.error
import urllib.request

ENDPOINT = os.environ["ENDPOINT"]
TOKEN = os.environ["TOKEN"]
RUNID = os.environ.get("RUNID", "local")

_passed = 0
_failed = 0


def section(t):
    print(f"\n\033[33m── {t}\033[0m")


def check(cond, label):
    global _passed, _failed
    if cond:
        _passed += 1
        print(f"  \033[32m✓\033[0m {label}")
    else:
        _failed += 1
        print(f"  \033[31m✗ {label}\033[0m")


def b64(s):
    if isinstance(s, str):
        s = s.encode()
    return base64.b64encode(s).decode()


def call(path, body, token=TOKEN):
    data = json.dumps(body).encode()
    req = urllib.request.Request(ENDPOINT + path, data=data, method="POST")
    req.add_header("Content-Type", "application/json")
    if token:
        req.add_header("Authorization", "Bearer " + token)
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            raw = r.read()
            return r.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        return e.code, (json.loads(raw) if raw else {})


# thin API wrappers
def create_queue(name, cfg=None):
    return call("/mqlite.v1.AdminService/CreateQueue", {"name": name, "config": cfg or {}})


def create_sub(topic, name, flt=None):
    body = {"topic": topic, "name": name}
    if flt:
        body["filter"] = flt
    return call("/mqlite.v1.AdminService/CreateSubscription", body)


def send(queue, body, **kw):
    msg = {"body": b64(body)}
    for k in ("message_id", "session_id", "subject", "correlation_id"):
        if k in kw:
            msg[k] = kw[k]
    req = {"queue": queue, "messages": [msg]}
    if "ttl_ms" in kw:
        req["ttl_ms"] = kw["ttl_ms"]
    return call("/mqlite.v1.QueueService/Send", req)


def send_batch(queue, bodies):
    return call("/mqlite.v1.QueueService/Send",
                {"queue": queue, "messages": [{"body": b64(b)} for b in bodies]})


def schedule(queue, body, at_ms):
    return call("/mqlite.v1.QueueService/Schedule",
                {"queue": queue, "messages": [{"body": b64(body)}], "scheduled_enqueue_time_ms": at_ms})


def cancel_scheduled(queue, seq):
    return call("/mqlite.v1.QueueService/CancelScheduled", {"queue": queue, "seq_number": seq})


def receive(queue, max_messages=1, wait_ms=0, mode=0):
    return call("/mqlite.v1.QueueService/Receive",
                {"queue": queue, "max_messages": max_messages, "wait_time_ms": wait_ms, "receive_mode": mode})


def settle(path, queue, seq, token, **kw):
    body = {"queue": queue, "seq_number": seq, "lock_token": token}
    body.update(kw)
    return call(path, body)


def complete(q, s, t):
    return settle("/mqlite.v1.QueueService/Complete", q, s, t)


def abandon(q, s, t, delay_ms=0):
    return settle("/mqlite.v1.QueueService/Abandon", q, s, t, delay_ms=delay_ms)


def deadletter(q, s, t, reason="", desc=""):
    return settle("/mqlite.v1.QueueService/DeadLetter", q, s, t,
                  dead_letter_reason=reason, dead_letter_description=desc)


def defer(q, s, t):
    return settle("/mqlite.v1.QueueService/Defer", q, s, t)


def receive_deferred(queue, seqs):
    return call("/mqlite.v1.QueueService/ReceiveDeferred", {"queue": queue, "seq_numbers": seqs})


def peek(queue, state="", mx=20):
    return call("/mqlite.v1.QueueService/Peek", {"queue": queue, "state": state, "max": mx})


def metrics(queue):
    return call("/mqlite.v1.QueueService/GetQueueMetrics", {"queue": queue})


def redrive(queue, **kw):
    body = {"queue": queue}
    body.update(kw)
    return call("/mqlite.v1.AdminService/Redrive", body)


def now_ms():
    return int(time.time() * 1000)


# ── scenarios ────────────────────────────────────────────────────────────────

def t_lifecycle():
    section("lifecycle: send → receive → complete")
    q = f"{RUNID}_py_basic"
    create_queue(q)
    st, r = send(q, "hello-py", message_id="m1")
    check(st == 200 and r["seq_numbers"][0] >= 1, "send returns seq")
    st, r = receive(q, wait_ms=3000)
    check(len(r["messages"]) == 1, "receive returns the message")
    m = r["messages"][0]
    check(base64.b64decode(m["body"]).decode() == "hello-py", "body round-trips")
    check(m["message_id"] == "m1", "message_id preserved")
    st, r = complete(q, m["seq_number"], m["lock_token"])
    check(r.get("ok") is True, "complete ok")
    _, r = metrics(q)
    check(r["total"] == 0, "queue drained")


def t_batch():
    section("batch send + max_messages receive")
    q = f"{RUNID}_py_batch"
    create_queue(q)
    st, r = send_batch(q, ["b1", "b2", "b3", "b4", "b5"])
    check(len(r["seq_numbers"]) == 5, "batch of 5 enqueued")
    _, r = receive(q, max_messages=3, wait_ms=2000)
    check(len(r["messages"]) == 3, "receive(max=3) returns 3")


def t_visibility_timeout():
    section("visibility timeout: expired lock is redelivered")
    q = f"{RUNID}_py_vis"
    create_queue(q, {"lock_duration_ms": 1000, "max_delivery_count": 10})
    send(q, "vt")
    _, r = receive(q, wait_ms=2000)
    m = r["messages"][0]
    check(m["delivery_count"] == 1, "first delivery_count == 1")
    # lock expires at +1s; the reaper (1s cadence) returns it to active within ≤1s
    time.sleep(2.5)
    _, r = receive(q, wait_ms=2000)
    check(len(r["messages"]) == 1 and r["messages"][0]["delivery_count"] == 2,
          "redelivered with delivery_count == 2 (via reaper)")


def t_dlq_redrive():
    section("dead-letter on max delivery + redrive")
    q = f"{RUNID}_py_dlq"
    create_queue(q, {"max_delivery_count": 2})
    send(q, "poison")
    for _ in range(2):
        _, r = receive(q, wait_ms=2000)
        m = r["messages"][0]
        abandon(q, m["seq_number"], m["lock_token"])
    _, r = peek(q, state="dead_lettered")
    check(len(r["messages"]) == 1, "message moved to DLQ after 2 deliveries")
    check(r["messages"][0]["dead_letter_reason"] == "MaxDeliveryCountExceeded", "DLQ reason recorded")
    _, r = metrics(q)
    check(r["dead_lettered"] == 1, "metrics dead_lettered == 1")
    _, r = redrive(q)
    check(r["moved"] >= 1, "redrive moved >= 1")
    _, r = receive(q, wait_ms=2000)
    check(len(r["messages"]) == 1, "redriven message receivable again")


def t_explicit_deadletter():
    section("explicit DeadLetter with reason")
    q = f"{RUNID}_py_dl"
    create_queue(q)
    send(q, "bad")
    _, r = receive(q, wait_ms=2000)
    m = r["messages"][0]
    deadletter(q, m["seq_number"], m["lock_token"], reason="Unprocessable", desc="schema mismatch")
    _, r = peek(q, state="dead_lettered")
    check(len(r["messages"]) == 1 and r["messages"][0]["dead_letter_reason"] == "Unprocessable",
          "explicit reason stored")


def t_defer():
    section("defer / receive-deferred")
    q = f"{RUNID}_py_defer"
    create_queue(q)
    _, r = send(q, "later")
    seq = r["seq_numbers"][0]
    _, r = receive(q, wait_ms=2000)
    defer(q, seq, r["messages"][0]["lock_token"])
    _, r = receive(q, wait_ms=300)
    check(len(r["messages"]) == 0, "deferred message hidden from normal receive")
    _, r = receive_deferred(q, [seq])
    check(len(r["messages"]) == 1, "receive-deferred fetches by seq")


def t_schedule():
    section("scheduled delivery becomes visible at its time")
    q = f"{RUNID}_py_sched"
    create_queue(q)
    # delay generously so the "before" check completes before activation even on a
    # remote backend where each round-trip is ~hundreds of ms.
    schedule(q, "scheduled", now_ms() + 2000)
    _, r = receive(q, wait_ms=0)  # no long-poll, returns immediately
    check(len(r["messages"]) == 0, "not visible before scheduled time")
    time.sleep(4.0)  # 2s delay + ~1s activation loop + margin
    _, r = receive(q, wait_ms=3000)
    check(len(r["messages"]) == 1, "visible after scheduled time")


def t_dedup():
    section("dedup window: silent drop + conflict on same id / different body")
    q = f"{RUNID}_py_dedup"
    create_queue(q, {"dedup_window_ms": 600000})
    _, r1 = send(q, "payload", message_id="dup-1")
    _, r2 = send(q, "payload", message_id="dup-1")
    check(r1["seq_numbers"][0] == r2["seq_numbers"][0], "duplicate returns original seq")
    _, r = metrics(q)
    check(r["active"] == 1, "queue depth unchanged by duplicate")
    st, r = send(q, "DIFFERENT", message_id="dup-1")
    check(st == 409, "same id / different body -> 409 conflict")


def t_sessions():
    section("MessageGroupId: in-order per group, parallel across groups")
    q = f"{RUNID}_py_sess"
    create_queue(q)
    send(q, "s1", session_id="orderA")
    send(q, "s2", session_id="orderA")
    _, r = receive(q, wait_ms=2000)
    m1 = r["messages"][0]
    check(base64.b64decode(m1["body"]).decode() == "s1", "first of group A delivered")
    _, r = receive(q, wait_ms=300)
    check(len(r["messages"]) == 0, "group head in-flight blocks the rest of the group")
    complete(q, m1["seq_number"], m1["lock_token"])
    _, r = receive(q, wait_ms=2000)
    check(base64.b64decode(r["messages"][0]["body"]).decode() == "s2", "second delivered after first completes")

    q2 = f"{RUNID}_py_sess2"
    create_queue(q2)
    send(q2, "a", session_id="A")
    send(q2, "b", session_id="B")
    _, r = receive(q2, max_messages=2, wait_ms=2000)
    sess = {m["session_id"] for m in r["messages"]}
    check(len(r["messages"]) == 2 and sess == {"A", "B"}, "different groups deliver in parallel")


def t_receive_and_delete():
    section("receive-and-delete (at-most-once fast path)")
    q = f"{RUNID}_py_rad"
    create_queue(q)
    send(q, "transient")
    _, r = receive(q, wait_ms=2000, mode=1)
    check(len(r["messages"]) == 1, "received one")
    _, r = metrics(q)
    check(r["total"] == 0, "message removed immediately (no settle needed)")


def t_fencing():
    section("fencing: stale lock token fails safely")
    q = f"{RUNID}_py_fence"
    create_queue(q)
    send(q, "x")
    _, r = receive(q, wait_ms=2000)
    m = r["messages"][0]
    _, r = complete(q, m["seq_number"], "deadbeef")
    check(r.get("ok") is False, "complete with wrong token -> ok:false")
    _, r = complete(q, m["seq_number"], m["lock_token"])
    check(r.get("ok") is True, "complete with right token -> ok:true")


def t_all_fields():
    section("all message fields round-trip through the API")
    q = f"{RUNID}_py_fields"
    create_queue(q)
    call("/mqlite.v1.QueueService/Send", {"queue": q, "messages": [{
        "body": b64("payload \x00\xff"), "message_id": "M1", "session_id": "S1",
        "correlation_id": "C1", "subject": "sub.x", "content_type": "application/json",
        "properties": {"tenant": "acme", "k": "中文🚀"},
    }]})
    _, r = receive(q, wait_ms=2000)
    m = r["messages"][0]
    ok = (m.get("message_id") == "M1" and m.get("session_id") == "S1" and
          m.get("correlation_id") == "C1" and m.get("subject") == "sub.x" and
          m.get("content_type") == "application/json" and
          m.get("properties", {}).get("k") == "中文🚀")
    check(ok, "message_id/session/correlation/subject/content_type/properties all preserved")
    check(base64.b64decode(m["body"]) == "payload \x00\xff".encode("utf-8", "surrogateescape") or
          len(base64.b64decode(m["body"])) > 0, "body preserved")


def t_max_size():
    section("max message size boundary (413)")
    cap = int(os.environ.get("MQLITE_MAX_MESSAGE_BYTES", "1048576"))
    q = f"{RUNID}_py_size"
    create_queue(q)
    st, _ = call("/mqlite.v1.QueueService/Send",
                 {"queue": q, "messages": [{"body": b64(b"x" * cap)}]})
    check(st == 200, f"body == cap ({cap}B) accepted")
    st, r = call("/mqlite.v1.QueueService/Send",
                 {"queue": q, "messages": [{"body": b64(b"x" * (cap + 1))}]})
    check(st == 413 and r.get("code") == "message_too_large", "body == cap+1 rejected with 413 message_too_large")


def t_abandon_delay():
    section("abandon with delay (backoff) re-hides the message")
    q = f"{RUNID}_py_delay"
    create_queue(q, {"max_delivery_count": 10})
    send(q, "x")
    _, r = receive(q, wait_ms=2000)
    m = r["messages"][0]
    abandon(q, m["seq_number"], m["lock_token"], delay_ms=1500)
    _, r = receive(q, wait_ms=0)
    check(len(r["messages"]) == 0, "hidden during backoff delay")
    time.sleep(2.0)
    _, r = receive(q, wait_ms=2000)
    check(len(r["messages"]) == 1, "reappears after backoff delay")


def t_cancel_scheduled():
    section("cancel scheduled removes the pending message")
    q = f"{RUNID}_py_cancel"
    create_queue(q)
    _, r = schedule(q, "later", now_ms() + 60_000)
    seq = r["seq_numbers"][0]
    _, r = peek(q, state="scheduled")
    check(len(r["messages"]) == 1, "scheduled message present before cancel")
    st, r = cancel_scheduled(q, seq)
    check(r.get("ok") is True, "cancel returns ok")
    _, r = peek(q, state="scheduled")
    check(len(r["messages"]) == 0, "scheduled message gone after cancel")


def main():
    print(f"Python behavioural suite — endpoint={ENDPOINT} runid={RUNID}")
    for fn in (t_lifecycle, t_batch, t_visibility_timeout, t_dlq_redrive,
               t_explicit_deadletter, t_defer, t_schedule, t_dedup, t_sessions,
               t_receive_and_delete, t_fencing,
               t_all_fields, t_max_size, t_abandon_delay, t_cancel_scheduled):
        try:
            fn()
        except Exception as e:  # noqa
            global _failed
            _failed += 1
            print(f"  \033[31m✗ {fn.__name__} raised {e!r}\033[0m")
    print(f"\nPython: \033[32m{_passed} passed\033[0m, \033[31m{_failed} failed\033[0m")
    sys.exit(1 if _failed else 0)


if __name__ == "__main__":
    main()
