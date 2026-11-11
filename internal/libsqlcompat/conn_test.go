package libsqlcompat

import (
	"context"
	"crypto/sha256"
	"database/sql/driver"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"

	libsql "github.com/tursodatabase/libsql-client-go/libsql"
)

func TestLegacyHTTPDSNValidationMatchesPinnedDriver(t *testing.T) {
	for _, dsn := range []string{
		"libsql://test.invalid", "libsql://test.invalid?authToken=fixture", "libsql://test.invalid?auth_token=fixture",
		"libsql://test.invalid?jwt=fixture", "libsql://test.invalid?auth_token=&authToken=fixture",
		"libsql://test.invalid?authToken=first&authToken=second", "libsql://test.invalid?authToken=one&jwt=two",
		"libsql://test.invalid?tls=0", "libsql://test.invalid:56789?tls=0", "libsql://test.invalid?tls=1",
		"libsql://test.invalid?tls=invalid", "https://test.invalid?tls=0", "http://test.invalid?tls=1",
		"http://test.invalid?tls=0", "https://test.invalid", "libsql://test.invalid?unknown=fixture",
		"libsql://test.invalid/%zz", "libsql://test.invalid#fragment",
	} {
		t.Run(dsn, func(t *testing.T) {
			original, originalErr := (libsql.Driver{}).Open(dsn)
			copied, copiedErr := OpenHTTP(dsn)
			if fmt.Sprint(originalErr) != fmt.Sprint(copiedErr) {
				t.Fatalf("validation changed: original=%v, compatibility=%v", originalErr, copiedErr)
			}
			if original != nil {
				_ = original.Close()
			}
			if copied != nil {
				_ = copied.Close()
			}
		})
	}
}

func TestLegacyHTTPDSNPreservesAuthHostAndRequestPath(t *testing.T) {
	type observed struct{ Auth, Host, Path, Query string }
	var mu sync.Mutex
	var requests []observed
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		_, _ = io.Copy(io.Discard, r.Body)
		mu.Lock()
		requests = append(requests, observed{r.Header.Get("Authorization"), r.Host, r.URL.Path, r.URL.RawQuery})
		mu.Unlock()
		_, _ = io.WriteString(w, `{"baton":null,"results":[{"type":"ok","response":{"type":"execute","result":{"cols":[],"rows":[],"affected_row_count":1}}}]}`)
	}))
	defer server.Close()
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"authToken", "auth_token", "jwt"} {
		dsn := "libsql://" + u.Host + "/custom?tls=0&" + key + "=" + url.QueryEscape("fixture +/? token")
		for _, open := range []func(string) (driver.Conn, error){(libsql.Driver{}).Open, OpenHTTP} {
			conn, err := open(dsn)
			if err != nil {
				t.Fatal(err)
			}
			_, err = conn.(driver.ExecerContext).ExecContext(context.Background(), "SELECT 1", nil)
			_ = conn.Close()
			if err != nil {
				t.Fatal(err)
			}
		}
	}
	mu.Lock()
	defer mu.Unlock()
	want := observed{"Bearer fixture +/? token", u.Host, "/custom/v2/pipeline", ""}
	if len(requests) != 6 {
		t.Fatalf("request count = %d", len(requests))
	}
	for i := range requests {
		if !reflect.DeepEqual(requests[i], want) {
			t.Fatalf("request %d changed: %#v", i, requests[i])
		}
	}
}

func replacePinnedBlock(t *testing.T, text, changed, original string) string {
	t.Helper()
	if strings.Count(text, changed) != 1 {
		t.Fatalf("review the exact approved HTTP compatibility delta: %q", changed)
	}
	return strings.Replace(text, changed, original, 1)
}

func TestCopiedRuntimeMatchesPinnedSourceOutsideApprovedCompatibilityDelta(t *testing.T) {
	var upstream struct {
		SourceCommit  string `json:"source_commit"`
		SourceVersion string `json:"source_version"`
		SourceModule  string `json:"source_module"`
		Files         []struct {
			Path string `json:"path"`
			SHA  string `json:"upstream_sha256"`
		} `json:"files"`
	}
	raw, err := os.ReadFile("UPSTREAM.json")
	if err != nil {
		t.Fatal(err)
	}
	manifestSum := sha256.Sum256(raw)
	if hex.EncodeToString(manifestSum[:]) != "7db9542df1b1f5cba867a42b96c372e5d1b01bd5f7f9336c4a2f928c3fb8c423" {
		t.Fatal("review the full pinned source manifest before updating its golden")
	}
	if err := json.Unmarshal(raw, &upstream); err != nil {
		t.Fatal(err)
	}
	if upstream.SourceCommit != "236aa1ff8accc5180a51c25b437c91630ac05221" || upstream.SourceVersion != "v0.0.0-20251219100830-236aa1ff8acc" || len(upstream.Files) != 13 {
		t.Fatal("review the complete upstream pin and compatibility inventory before changing it")
	}
	module, err := os.ReadFile(filepath.Join("..", "..", "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if !matchesPinnedParserDependency(string(module), upstream.SourceModule, upstream.SourceVersion) {
		t.Fatal("review the copied driver together with its exact public parser dependency; duplicate or replaced pins are forbidden")
	}
	seen := map[string]bool{}
	for _, file := range upstream.Files {
		if seen[file.Path] || !filepath.IsLocal(file.Path) {
			t.Fatalf("invalid copied source inventory: %q", file.Path)
		}
		seen[file.Path] = true
		raw, err := os.ReadFile(file.Path)
		if err != nil {
			t.Fatal(err)
		}
		text := strings.NewReplacer(
			"github.com/mqlitehq/mqlite/internal/libsqlcompat/hrana", "github.com/tursodatabase/libsql-client-go/libsql/internal/hrana",
			"github.com/mqlitehq/mqlite/internal/libsqlcompat/shared", "github.com/tursodatabase/libsql-client-go/libsql/internal/http/shared",
		).Replace(string(raw))
		if file.Path == "httpv2/hranaV2.go" {
			text = restorePinnedHTTPLifecycle(t, text)
		}
		text = restorePinnedAcknowledgements(t, file.Path, text)
		sum := sha256.Sum256([]byte(text))
		if hex.EncodeToString(sum[:]) != file.SHA {
			t.Fatalf("unreviewed upstream protocol/SQL/result delta in %s", file.Path)
		}
	}
}

// Remove only the reviewed acknowledgement checks. Every remaining copied byte
// must still match the original driver, including its SQL and retry behavior.
func restorePinnedAcknowledgements(t *testing.T, path, text string) string {
	t.Helper()
	switch path {
	case "httpv2/hranaV2.go":
		text = replacePinnedBlock(t, text,
			`return hrana.PipelineResponse{}, false, hrana.IncompleteAcknowledgement(err)`,
			`return hrana.PipelineResponse{}, false, err`)
		text = replacePinnedBlock(t, text,
			`return hrana.PipelineResponse{}, false, hrana.InvalidAcknowledgement(err)`,
			`return hrana.PipelineResponse{}, false, err`)
		text = replacePinnedBlock(t, text, `	if err := validateAcknowledgement(msg, result); err != nil {
		return nil, err
	}
`, "")
		text = replacePinnedBlock(t, text, `			if r == nil {
				continue // A conditional step may legitimately be skipped.
			}
`, "")
	case "hrana/stream_result.go":
		text = replacePinnedBlock(t, text, `	// RequestedBatch is set from the sent request, never from provider JSON.
	RequestedBatch *Batch `+"`json:\"-\"`"+`
`, "")
		const wrapped = `return nil, InvalidAcknowledgement(err)`
		if strings.Count(text, wrapped) != 2 {
			t.Fatal("review both statement and batch acknowledgement decode errors")
		}
		text = strings.ReplaceAll(text, wrapped, `return nil, err`)
		const objectCheck = `	if err := validateResultObject(r.Result); err != nil {
		return nil, err
	}
`
		if strings.Count(text, objectCheck) != 2 {
			t.Fatal("review both statement and batch result object checks")
		}
		text = strings.ReplaceAll(text, objectCheck, "")
		text = replacePinnedBlock(t, text, `	if err := validateBatchAcknowledgement(r, &res); err != nil {
		return nil, err
	}
`, "")
	case "hrana/stmt_result.go":
		text = replacePinnedBlock(t, text, "\t\tAffectedRowCount *int32      `json:\"affected_row_count\"`\n", "")
		text = replacePinnedBlock(t, text, `	if err := validateStatementAcknowledgement(r, aux.AffectedRowCount); err != nil {
		return err
	}
`, "")
	}
	return text
}

func matchesPinnedParserDependency(moduleFile, module, version string) bool {
	inRequire, count := false, 0
	for _, line := range strings.Split(moduleFile, "\n") {
		fields := strings.Fields(strings.SplitN(line, "//", 2)[0])
		if len(fields) == 0 {
			continue
		}
		if fields[0] == "replace" {
			return false
		}
		if fields[0] == "require" {
			fields = fields[1:]
			if len(fields) == 1 && fields[0] == "(" {
				inRequire = true
				continue
			}
		} else if !inRequire {
			continue
		}
		if len(fields) == 0 {
			continue
		}
		if fields[0] == ")" {
			inRequire = false
			continue
		}
		if fields[0] == module {
			if len(fields) != 2 || fields[1] != version {
				return false
			}
			count++
		}
	}
	return count == 1
}

func TestParserDependencyGuardRejectsUpgradesDuplicatesAndReplacement(t *testing.T) {
	const module = "github.com/tursodatabase/libsql-client-go"
	const version = "v0.0.0-20251219100830-236aa1ff8acc"
	const dependency = module + " " + version
	for _, input := range []string{"require " + dependency, "require (\n\t" + dependency + " // pinned parser\n)"} {
		if !matchesPinnedParserDependency(input, module, version) {
			t.Fatalf("valid pin was rejected: %q", input)
		}
	}
	for _, input := range []string{
		"require " + module + " v0.0.0-new",
		"require (\n" + dependency + "\n" + dependency + "\n)",
		"require " + dependency + "\nreplace " + module + " => ../changed-driver",
		"require " + dependency + "\nreplace (\n" + module + " => example.invalid/fork v1.0.0\n)",
	} {
		if matchesPinnedParserDependency(input, module, version) {
			t.Fatalf("changed parser dependency escaped review: %q", input)
		}
	}
}

// Restore only these explicitly reviewed constructor/ownership changes before
// comparing every copied byte with the original pinned source checksum.
func restorePinnedHTTPLifecycle(t *testing.T, text string) string {
	t.Helper()
	text = replacePinnedBlock(t, text, `func Connect(url, jwt, host string, schemaDb bool, encryptionKey string, cleanup *CleanupManager) driver.Conn {
	return &hranaV2Conn{url, jwt, host, schemaDb, encryptionKey, "", false, 0, cleanup}
}`, `func Connect(url, jwt, host string, schemaDb bool, encryptionKey string) driver.Conn {
	return &hranaV2Conn{url, jwt, host, schemaDb, encryptionKey, "", false, 0}
}`)
	text = replacePinnedBlock(t, text, `type hranaV2Conn struct {
	url                 string
	jwt                 string
	host                string
	schemaDb            bool
	remoteEncryptionKey string
	baton               string
	streamClosed        bool
	replicationIndex    uint64
	cleanup             *CleanupManager
}`, `type hranaV2Conn struct {
	url                 string
	jwt                 string
	host                string
	schemaDb            bool
	remoteEncryptionKey string
	baton               string
	streamClosed        bool
	replicationIndex    uint64
}`)
	text = replacePinnedBlock(t, text, `func (h *hranaV2Conn) Close() error {
	h.closeStream()
	return nil
}`, `func (h *hranaV2Conn) Close() error {
	if h.baton != "" {
		go func(baton, url, jwt, host, encryptionKey string) {
			msg := hrana.PipelineRequest{Baton: baton}
			msg.Add(hrana.CloseStream())
			_, _, _ = sendPipelineRequest(context.Background(), &msg, url, jwt, host, encryptionKey)
		}(h.baton, h.url, h.jwt, h.host, h.remoteEncryptionKey)
	}
	return nil
}`)
	text = replacePinnedBlock(t, text, `func (h *hranaV2Conn) closeStream() {
	if h.baton != "" {
		baton := h.baton
		h.baton = ""
		h.cleanup.closeStreamAsync(baton, h.url, h.jwt, h.host, h.remoteEncryptionKey)
	}
}`, `func (h *hranaV2Conn) closeStream() {
	if h.baton != "" {
		go func(baton, url, jwt, host, encryptionKey string) {
			msg := hrana.PipelineRequest{Baton: baton}
			msg.Add(hrana.CloseStream())
			_, _, _ = sendPipelineRequest(context.Background(), &msg, url, jwt, host, encryptionKey)
		}(h.baton, h.url, h.jwt, h.host, h.remoteEncryptionKey)
		h.baton = ""
	}
}`)
	text = replacePinnedBlock(t, text, `	if resp.StatusCode != http.StatusOK {
		defer func() {
			if err != nil {
				err = &providerHTTPError{status: resp.StatusCode, cause: err}
			}
		}()
	}
`, "")
	text = replacePinnedBlock(t, text, "return nil, providerExecutionError(msg, result, r)", "return nil, errors.New(r.Error.Message)")
	return text
}
