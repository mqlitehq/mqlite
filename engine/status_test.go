package engine

import (
	"context"
	"strings"
	"testing"
)

func TestStatusLocalFile(t *testing.T) {
	e, _ := testEngine(t)
	st := e.Status(context.Background())
	if st.Backend != "local file" || st.Remote {
		t.Errorf("backend=%q remote=%v, want local file", st.Backend, st.Remote)
	}
	if !strings.HasSuffix(st.Location, "mq.db") {
		t.Errorf("location=%q, want a path ending in mq.db", st.Location)
	}
	if st.SizeBytes <= 0 {
		t.Errorf("size=%d, want >0 (schema written to disk)", st.SizeBytes)
	}
	if st.PingMs < 0 {
		t.Errorf("ping=%d, want >=0", st.PingMs)
	}
	if st.SchemaVersion != schemaVersion {
		t.Errorf("schema=%q, want %q", st.SchemaVersion, schemaVersion)
	}
}

func TestStatusMemory(t *testing.T) {
	e, err := Open(context.Background(), Options{DB: ":memory:", DisableBackground: true})
	if err != nil {
		t.Fatal(err)
	}
	defer e.Close()
	st := e.Status(context.Background())
	if st.Backend != "memory" || st.Location != ":memory:" || st.SizeBytes != 0 {
		t.Errorf("memory status = %+v", st)
	}
}

func TestRedactRemoteDSN(t *testing.T) {
	cases := map[string]string{
		"libsql://mydb.turso.io":     "libsql://***.turso.io",
		"libsql://mydb-org.turso.io": "libsql://***.turso.io",
		"wss://host.example.com":     "wss://***.example.com",
		"libsql://single":            "libsql://***",
		"libsql://mydb.turso.io?x=1": "libsql://***.turso.io",
	}
	for in, want := range cases {
		if got := redactRemoteDSN(in); got != want {
			t.Errorf("redactRemoteDSN(%q) = %q, want %q", in, got, want)
		}
	}
	// the database-identifying label must never survive the masking.
	if strings.Contains(redactRemoteDSN("libsql://secret-db.turso.io"), "secret-db") {
		t.Error("redaction leaked the database name")
	}
}
