package engine

// expr subscription-filter tests (MQLITE-17). Sections:
//
//   - Duration units      parseDuration incl. d/w + composites/fractions/sign
//   - Compile             empty=match-all, syntax/unknown-field/non-bool/too-long rejected
//   - Field matching      core / properties / subject_parts / body_size / property_keys
//   - Time semantics      enqueued_at/visible_at windows + delay-by-subtraction + duration helpers
//   - Fail-closed         a runtime error never matches and never panics the host
//   - Fan-out             many messages × many subscriptions, every condition triggered
//   - Scheduled messages  visible_at - enqueued_at routes a delayed send differently
//   - Subscribe validation a bad expr is rejected (ErrInvalidFilter) and creates no queue
//   - Re-subscribe        a changed filter recompiles (cache invalidation)
//   - Transactional path  filters apply on the outbox (EngineTx) fan-out too

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/expr-lang/expr/vm"
)

// ─── Duration units ─────────────────────────────────────────────────────────

func TestParseDurationUnits(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
		bad  bool
	}{
		{"1h", time.Hour, false},
		{"90m", 90 * time.Minute, false},
		{"500ms", 500 * time.Millisecond, false},
		{"1d", 24 * time.Hour, false},
		{"2w", 14 * 24 * time.Hour, false},
		{"1d12h", 36 * time.Hour, false},
		{"1.5d", 36 * time.Hour, false},
		{"1w3d", (7 + 3) * 24 * time.Hour, false},
		{"-1h", -time.Hour, false},
		{"0", 0, false},
		{"", 0, true},
		{"1y", 0, true},  // no year unit (calendar-ambiguous)
		{"1mo", 0, true}, // no month unit
		{"abc", 0, true},
		{"1x", 0, true},
	}
	for _, c := range cases {
		got, err := parseDuration(c.in)
		if c.bad {
			if err == nil {
				t.Errorf("parseDuration(%q) = %v, want error", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseDuration(%q): unexpected error %v", c.in, err)
		} else if got != c.want {
			t.Errorf("parseDuration(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

// ─── Compile ────────────────────────────────────────────────────────────────

func TestCompileFilter(t *testing.T) {
	// Empty / whitespace = match-all (nil program, no error).
	for _, empty := range []string{"", "   "} {
		p, err := compileFilter(empty)
		if err != nil || p != nil {
			t.Fatalf("compileFilter(%q): prog=%v err=%v, want nil/nil", empty, p, err)
		}
	}
	bad := map[string]string{
		"syntax":         `subject ==`,
		"unknown field":  `nope == "x"`,
		"unknown func":   `frobnicate(subject)`,
		"non-bool int":   `body_size + 1`,
		"non-bool str":   `subject`,
		"too long":       strings.Repeat("(subject=='a')||", 400) + "subject=='a'", // > maxFilterNodes or len
		"source too big": strings.Repeat("a", maxFilterSourceLen+1),
	}
	for name, src := range bad {
		if _, err := compileFilter(src); !errors.Is(err, ErrInvalidFilter) {
			t.Errorf("%s: want ErrInvalidFilter, got %v", name, err)
		}
	}
	// A valid expression compiles to a runnable program.
	if p, err := compileFilter(`subject_parts[0] == "orders"`); err != nil || p == nil {
		t.Fatalf("valid filter: prog=%v err=%v", p, err)
	}
}

// ─── Field matching ─────────────────────────────────────────────────────────

func TestFilterFieldMatching(t *testing.T) {
	now := int64(1_700_000_000_000)
	base := OutMessage{
		Subject:       "orders.eu.new",
		Properties:    map[string]string{"tier": "gold", "region": "eu"},
		GroupID:       "g1",
		CorrelationID: "c1",
		ReplyTo:       "r1",
		MessageID:     "m1",
		ContentType:   "application/json",
		Body:          []byte("hello"),
	}
	cases := []struct {
		name, expr string
		want       bool
	}{
		{"subject eq", `subject == "orders.eu.new"`, true},
		{"subject parts head", `subject_parts[0] == "orders"`, true},
		{"subject parts len", `len(subject_parts) == 3`, true},
		{"subject startsWith", `subject startsWith "orders."`, true},
		{"subject matches", `subject matches "^orders"`, true},
		{"property index", `properties["tier"] == "gold"`, true},
		{"property in", `"region" in properties`, true},
		{"property absent in", `"missing" in properties`, false},
		{"property missing is empty", `properties["missing"] == ""`, true},
		{"and", `properties["tier"] == "gold" && subject_parts[1] == "eu"`, true},
		{"or false", `properties["tier"] == "silver" || subject_parts[0] == "payments"`, false},
		{"not", `not (properties["muted"] == "true")`, true},
		{"group_id", `group_id == "g1"`, true},
		{"correlation_id", `correlation_id == "c1"`, true},
		{"reply_to", `reply_to == "r1"`, true},
		{"message_id", `message_id == "m1"`, true},
		{"content_type", `content_type == "application/json"`, true},
		{"body_size eq", `body_size == 5`, true},
		{"body_size lt", `body_size < 4096`, true},
		{"property_keys len", `len(property_keys) == 2`, true},
		{"property_keys contains", `"tier" in property_keys`, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			prog, err := compileFilter(c.expr)
			if err != nil {
				t.Fatalf("compile %q: %v", c.expr, err)
			}
			got, err := evalFilter(prog, buildFilterEnv(base, now, now))
			if err != nil {
				t.Fatalf("eval %q: %v", c.expr, err)
			}
			if got != c.want {
				t.Errorf("%q = %v, want %v", c.expr, got, c.want)
			}
		})
	}
}

// nil/empty properties must not panic and must read as absent.
func TestFilterNilProperties(t *testing.T) {
	m := OutMessage{Subject: "x", Body: nil} // no properties, empty body
	for _, expr := range []string{
		`properties["x"] == ""`,
		`!("x" in properties)`,
		`len(property_keys) == 0`,
		`body_size == 0`,
		`len(subject_parts) == 1`,
	} {
		prog, err := compileFilter(expr)
		if err != nil {
			t.Fatalf("compile %q: %v", expr, err)
		}
		got, err := evalFilter(prog, buildFilterEnv(m, 0, 0))
		if err != nil || !got {
			t.Errorf("%q on empty message = %v (err %v), want true", expr, got, err)
		}
	}
	// empty subject yields an empty (not [""]) hierarchy
	got, _ := evalFilter(mustCompile(t, `len(subject_parts) == 0`), buildFilterEnv(OutMessage{}, 0, 0))
	if !got {
		t.Error("empty subject should give zero subject_parts")
	}
}

// ─── Time semantics ─────────────────────────────────────────────────────────

func TestFilterTimeSemantics(t *testing.T) {
	enq := time.Date(2026, 6, 22, 10, 0, 0, 0, time.UTC).UnixMilli()
	vis2h := time.Date(2026, 6, 22, 12, 0, 0, 0, time.UTC).UnixMilli()
	m := OutMessage{Subject: "x", Body: []byte("b")}
	cases := []struct {
		name, expr string
		enq, vis   int64
		want       bool
	}{
		{"enqueued hour", `enqueued_at.Hour() == 10`, enq, enq, true},
		{"visible hour", `visible_at.Hour() == 12`, enq, vis2h, true},
		{"publish window", `enqueued_at.Hour() >= 9 && enqueued_at.Hour() <= 21`, enq, enq, true},
		{"delay via duration h", `visible_at - enqueued_at > duration("1h")`, enq, vis2h, true},
		{"delay via hours()", `visible_at - enqueued_at == hours(2)`, enq, vis2h, true},
		{"delay not over a day", `visible_at - enqueued_at > days(1)`, enq, vis2h, false},
		{"immediate has no delay", `visible_at - enqueued_at == seconds(0)`, enq, enq, true},
		{"under a day", `visible_at - enqueued_at < duration("1d")`, enq, vis2h, true},
		// constant-folding sanity for the custom duration units / helpers
		{"week equals 7 days", `duration("1w") == days(7)`, enq, enq, true},
		{"composite duration", `duration("1d12h") == hours(36)`, enq, enq, true},
		{"minutes helper", `minutes(60) == hours(1)`, enq, enq, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := evalFilter(mustCompile(t, c.expr), buildFilterEnv(m, c.enq, c.vis))
			if err != nil {
				t.Fatalf("eval %q: %v", c.expr, err)
			}
			if got != c.want {
				t.Errorf("%q = %v, want %v", c.expr, got, c.want)
			}
		})
	}
}

// ─── Fail-closed ────────────────────────────────────────────────────────────

// A runtime error (here: an out-of-range subject_parts index — the realistic
// footgun of a filter written for a deeper subject hierarchy than the message has)
// must yield (false, err): never a match, never a host panic.
func TestEvalFilterFailClosed(t *testing.T) {
	prog := mustCompile(t, `subject_parts[5] == "x"`)
	ok, err := evalFilter(prog, buildFilterEnv(OutMessage{Subject: "orders"}, 0, 0)) // 1 part only
	if ok {
		t.Error("fail-closed must not match on a runtime error")
	}
	if err == nil {
		t.Error("expected the runtime error to be reported for logging")
	}
	// The same filter evaluates cleanly when the hierarchy is deep enough.
	ok2, err2 := evalFilter(prog, buildFilterEnv(OutMessage{Subject: "a.b.c.d.e.x"}, 0, 0))
	if err2 != nil || !ok2 {
		t.Errorf("deep subject should match: ok=%v err=%v", ok2, err2)
	}
}

// ─── Fan-out: every condition, against a batch of existing messages ─────────

func TestFilterFanoutConditions(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	sub := func(name, expr string) {
		t.Helper()
		var f *Filter
		if expr != "" {
			f = &Filter{Expr: expr}
		}
		if err := e.Subscribe(ctx, "ev", name, f); err != nil {
			t.Fatalf("subscribe %s: %v", name, err)
		}
	}
	sub("all", "") // empty filter = match all
	sub("orders", `subject_parts[0] == "orders"`)
	sub("gold", `properties["tier"] == "gold"`)
	sub("small", `body_size < 8`)
	sub("eu_gold", `subject_parts[0] == "orders" && properties["region"] == "eu" && properties["tier"] == "gold"`)

	msgs := []OutMessage{
		{Subject: "orders.eu.new", Properties: map[string]string{"tier": "gold", "region": "eu"}, Body: []byte("x")},
		{Subject: "orders.us.new", Properties: map[string]string{"tier": "silver", "region": "us"}, Body: []byte("x")},
		{Subject: "payments.eu", Properties: map[string]string{"tier": "gold", "region": "eu"}, Body: []byte("bigggggbody")},
		{Subject: "orders.eu.big", Properties: map[string]string{"tier": "gold", "region": "eu"}, Body: []byte("bigggggbody")},
	}
	for _, m := range msgs {
		if _, err := e.SendOne(ctx, "ev", m); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	want := map[string]int64{"all": 4, "orders": 3, "gold": 3, "small": 2, "eu_gold": 2}
	for name, n := range want {
		st, err := e.Stats(ctx, name)
		if err != nil {
			t.Fatalf("stats %s: %v", name, err)
		}
		if st.Active != n {
			t.Errorf("subscription %q active=%d, want %d", name, st.Active, n)
		}
	}
}

// ─── Scheduled messages: routing on the enqueue→visible delay ──────────────

func TestFilterScheduledMessageDelay(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	now := e.now()

	if err := e.Subscribe(ctx, "sev", "delayed", &Filter{Expr: `visible_at - enqueued_at > hours(1)`}); err != nil {
		t.Fatal(err)
	}
	if err := e.Subscribe(ctx, "sev", "immediate", &Filter{Expr: `visible_at - enqueued_at == seconds(0)`}); err != nil {
		t.Fatal(err)
	}

	// An immediate send: visible_at == enqueued_at.
	if _, err := e.SendOne(ctx, "sev", OutMessage{Subject: "a", Body: []byte("i")}); err != nil {
		t.Fatalf("send: %v", err)
	}
	// A scheduled send 2h ahead: visible_at - enqueued_at == 2h.
	if _, err := e.Schedule(ctx, "sev", OutMessage{Subject: "a", Body: []byte("s")}, now+(2*time.Hour).Milliseconds()); err != nil {
		t.Fatalf("schedule: %v", err)
	}

	dl, _ := e.Stats(ctx, "delayed")
	im, _ := e.Stats(ctx, "immediate")
	// "delayed" got only the scheduled message (still in scheduled state).
	if dl.Total != 1 || dl.Scheduled != 1 {
		t.Errorf("delayed: total=%d scheduled=%d, want 1/1", dl.Total, dl.Scheduled)
	}
	// "immediate" got only the immediate message (active).
	if im.Total != 1 || im.Active != 1 {
		t.Errorf("immediate: total=%d active=%d, want 1/1", im.Total, im.Active)
	}
}

// ─── Subscribe-time validation ─────────────────────────────────────────────

func TestFilterBadExprRejectedAtSubscribe(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	for name, bad := range map[string]string{
		"syntax":        `subject ==`,
		"unknown field": `nope == "x"`,
		"non-bool":      `body_size + 1`,
	} {
		if err := e.Subscribe(ctx, "ev", "rej_"+name, &Filter{Expr: bad}); !errors.Is(err, ErrInvalidFilter) {
			t.Errorf("%s: want ErrInvalidFilter, got %v", name, err)
		}
	}
	// A rejected subscription must not have created its backing queue.
	if _, err := e.Stats(ctx, "rej_syntax"); !errors.Is(err, ErrQueueNotFound) {
		t.Errorf("rejected subscription must not create a queue, got %v", err)
	}
}

// ─── Re-subscribe recompiles (cache invalidation) ──────────────────────────

func TestFilterReSubscribeRecompiles(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	if err := e.Subscribe(ctx, "ev", "s", &Filter{Expr: `subject == "a"`}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.SendOne(ctx, "ev", OutMessage{Subject: "a"}); err != nil {
		t.Fatal(err)
	}
	if st, _ := e.Stats(ctx, "s"); st.Active != 1 {
		t.Fatalf("phase 1: active=%d, want 1", st.Active)
	}
	// Re-subscribe the same name with a different filter.
	if err := e.Subscribe(ctx, "ev", "s", &Filter{Expr: `subject == "b"`}); err != nil {
		t.Fatal(err)
	}
	// "a" must now be rejected (proves the cache recompiled, not the stale program).
	if _, err := e.SendOne(ctx, "ev", OutMessage{Subject: "a"}); err != nil {
		t.Fatal(err)
	}
	if st, _ := e.Stats(ctx, "s"); st.Active != 1 {
		t.Fatalf("after re-subscribe, 'a' should be rejected: active=%d, want 1", st.Active)
	}
	// "b" now matches.
	if _, err := e.SendOne(ctx, "ev", OutMessage{Subject: "b"}); err != nil {
		t.Fatal(err)
	}
	if st, _ := e.Stats(ctx, "s"); st.Active != 2 {
		t.Fatalf("after re-subscribe, 'b' should match: active=%d, want 2", st.Active)
	}
}

// ─── Transactional outbox path applies filters too ─────────────────────────

func TestFilterInTransaction(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	if err := e.Subscribe(ctx, "tev", "gold", &Filter{Expr: `properties["tier"] == "gold"`}); err != nil {
		t.Fatal(err)
	}
	if err := e.Subscribe(ctx, "tev", "all", nil); err != nil {
		t.Fatal(err)
	}
	err := e.Tx(ctx, func(tx *EngineTx) error {
		if _, err := tx.SendOne("tev", OutMessage{Subject: "a", Properties: map[string]string{"tier": "gold"}}); err != nil {
			return err
		}
		_, err := tx.SendOne("tev", OutMessage{Subject: "b", Properties: map[string]string{"tier": "silver"}})
		return err
	})
	if err != nil {
		t.Fatalf("tx: %v", err)
	}
	if g, _ := e.Stats(ctx, "gold"); g.Active != 1 {
		t.Errorf("gold (tx) active=%d, want 1", g.Active)
	}
	if a, _ := e.Stats(ctx, "all"); a.Active != 2 {
		t.Errorf("all (tx) active=%d, want 2", a.Active)
	}
}

// mustCompile compiles a filter or fails the test.
func mustCompile(t *testing.T, src string) *vm.Program {
	t.Helper()
	prog, err := compileFilter(src)
	if err != nil {
		t.Fatalf("compile %q: %v", src, err)
	}
	return prog
}

// ─── Body content: body_text / body_json (MQLITE-47) ────────────────────────

// evalBody mirrors the engine fan-out path: it projects the body fields only when
// the compiled filter references them (the cost gate), then evaluates.
func evalBody(t *testing.T, src string, m OutMessage) (bool, error) {
	t.Helper()
	prog := mustCompile(t, src)
	env := buildFilterEnv(m, 0, 0)
	if referencesVar(prog, "body_text") {
		env.BodyText = string(m.Body)
	}
	if referencesVar(prog, "body_json") {
		env.BodyJSON = decodeBodyJSON(m)
	}
	return evalFilter(prog, env)
}

func TestFilterBodyJSON(t *testing.T) {
	j := OutMessage{
		ContentType: "application/json",
		Body:        []byte(`{"amount":150,"tier":"gold","nested":{"k":"v"},"tags":["a","b"]}`),
	}
	cases := []struct {
		name, src string
		want      bool
	}{
		{"dot gt", `body_json.amount > 100`, true},
		{"dot lt false", `body_json.amount < 100`, false},
		{"index access", `body_json["tier"] == "gold"`, true},
		{"nested", `body_json.nested.k == "v"`, true},
		{"array elem", `body_json.tags[0] == "a"`, true},
		{"key present", `"amount" in body_json`, true},
		{"key absent", `"missing" in body_json`, false},
		{"len", `len(body_json) == 4`, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := evalBody(t, c.src, j)
			if err != nil {
				t.Fatalf("%q: %v", c.src, err)
			}
			if got != c.want {
				t.Errorf("%q = %v, want %v", c.src, got, c.want)
			}
		})
	}
}

// Absent fields, non-JSON content types, and non-object JSON are total-valued ({})
// or fail-closed — never a nil panic, never a default match.
func TestFilterBodyJSONEdgeCases(t *testing.T) {
	// Reaching into an absent field then comparing numerically must NOT match
	// (fail-closed: nil compare yields false, with or without a reported error).
	if got, _ := evalBody(t, `body_json.amount > 100`,
		OutMessage{ContentType: "application/json", Body: []byte(`{"other":1}`)}); got {
		t.Error("absent field comparison must not match")
	}
	// A presence check on an empty object is total-valued: false, no error.
	if got, err := evalBody(t, `"amount" in body_json`,
		OutMessage{ContentType: "application/json", Body: []byte(`{}`)}); got || err != nil {
		t.Errorf("absent-key presence: got=%v err=%v, want false/nil", got, err)
	}
	// An explicit non-JSON content type is respected → body_json stays {}.
	if got, err := evalBody(t, `"amount" in body_json`,
		OutMessage{ContentType: "text/plain", Body: []byte(`{"amount":1}`)}); got || err != nil {
		t.Errorf("non-JSON content type must not parse: got=%v err=%v", got, err)
	}
	// Invalid JSON and non-object JSON (array) both fall back to {}.
	for _, body := range []string{`not json`, `[1,2,3]`, `42`, ``} {
		if got, err := evalBody(t, `len(body_json) == 0`,
			OutMessage{ContentType: "application/json", Body: []byte(body)}); !got || err != nil {
			t.Errorf("body %q → empty map expected: got=%v err=%v", body, got, err)
		}
	}
}

func TestFilterBodyText(t *testing.T) {
	m := OutMessage{Body: []byte("ALERT: disk full urgent")}
	cases := []struct {
		src  string
		want bool
	}{
		{`body_text contains "urgent"`, true},
		{`body_text startsWith "ALERT"`, true},
		{`body_text contains "ok"`, false},
		{`len(body_text) > 0`, true},
	}
	for _, c := range cases {
		got, err := evalBody(t, c.src, m)
		if err != nil || got != c.want {
			t.Errorf("%q = %v (err %v), want %v", c.src, got, err, c.want)
		}
	}
	// Empty body → body_text is "" (total-valued).
	if got, err := evalBody(t, `body_text == ""`, OutMessage{}); err != nil || !got {
		t.Errorf("empty body → empty body_text: got=%v err=%v", got, err)
	}
}

// The body is projected only when the filter references it.
func TestFilterBodyGate(t *testing.T) {
	if !referencesVar(mustCompile(t, `body_json.x == 1`), "body_json") {
		t.Error("body_json use not detected")
	}
	if !referencesVar(mustCompile(t, `body_text contains "x"`), "body_text") {
		t.Error("body_text use not detected")
	}
	if referencesVar(mustCompile(t, `subject == "x"`), "body_json") {
		t.Error("false positive: body_json on a subject filter")
	}
	if referencesVar(mustCompile(t, `properties["k"] == "v"`), "body_text") {
		t.Error("false positive: body_text on a property filter")
	}
	// A filter that doesn't reference the body must work even when the body is
	// invalid JSON — because it is never parsed (the gate is what makes this safe).
	if got, err := evalBody(t, `subject == "x"`,
		OutMessage{Subject: "x", ContentType: "application/json", Body: []byte("garbage{")}); err != nil || !got {
		t.Errorf("non-body filter must ignore the body: got=%v err=%v", got, err)
	}
}

// Through the engine: body filters route fan-out over a batch of real messages.
func TestFilterBodyFanout(t *testing.T) {
	ctx := context.Background()
	e, _ := testEngine(t)
	sub := func(name, expr string) {
		t.Helper()
		var f *Filter
		if expr != "" {
			f = &Filter{Expr: expr}
		}
		if err := e.Subscribe(ctx, "bev", name, f); err != nil {
			t.Fatalf("subscribe %s: %v", name, err)
		}
	}
	sub("all", "")
	sub("big", `body_json.amount > 100`)
	sub("urgent", `body_text contains "urgent"`)

	send := func(ct, body string) {
		if _, err := e.SendOne(ctx, "bev", OutMessage{ContentType: ct, Body: []byte(body)}); err != nil {
			t.Fatalf("send: %v", err)
		}
	}
	send("application/json", `{"amount":150}`)                  // big
	send("application/json", `{"amount":50}`)                   // neither
	send("text/plain", "this is urgent")                        // urgent (body_json stays {})
	send("application/json", `{"amount":200,"note":"urgent!"}`) // big + urgent

	for name, n := range map[string]int64{"all": 4, "big": 2, "urgent": 2} {
		if st, _ := e.Stats(ctx, name); st.Active != n {
			t.Errorf("subscription %q active=%d, want %d", name, st.Active, n)
		}
	}
}
