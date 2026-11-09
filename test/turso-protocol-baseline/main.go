// A read-only HTTP/Hrana latency control. It never opens the MQLite engine.
package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"time"
)

var approved = map[string]string{
	"standard": "b72bfdad8f3e7696b3d216a2e947705b8efbd6c183e77f44d2cdcb16cef544b3",
	"rust":     "3f0ad22a38621cd9fd112e1e249d97d093ab53b407eba7b897ccfe7c0e3ffe17",
}

//go:embed main.go
var programSource []byte

type sample struct {
	Index        int     `json:"index"`
	ElapsedMS    float64 `json:"elapsed_ms"`
	ConnectionMS float64 `json:"http_connection_wait_ms"`
	DNSMS        float64 `json:"dns_ms"`
	ConnectMS    float64 `json:"tcp_connect_ms"`
	TLSMS        float64 `json:"tls_ms"`
	TTFBMS       float64 `json:"ttfb_ms"`
	BodyMS       float64 `json:"body_ms"`
	Reused       bool    `json:"connection_reused"`
	Status       int     `json:"http_status"`
	Class        string  `json:"result_class"`
	ResultOne    bool    `json:"select_one_verified"`
}

type report struct {
	Scope       string   `json:"scope"`
	Backend     string   `json:"backend"`
	Go          string   `json:"go"`
	OS          string   `json:"os"`
	Arch        string   `json:"arch"`
	ProgramSHA  string   `json:"program_sha256"`
	StartedAt   string   `json:"started_at"`
	FinishedAt  string   `json:"finished_at"`
	ProxySet    bool     `json:"proxy_environment_present"`
	Samples     []sample `json:"samples"`
	Passed      bool     `json:"passed"`
	Limitations []string `json:"limitations"`
}

func errorClass(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "deadline"
	}
	if errors.Is(err, context.Canceled) {
		return "canceled"
	}
	var dns *net.DNSError
	if errors.As(err, &dns) {
		return "dns"
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return "eof"
	}
	return "transport"
}

func request(ctx context.Context, client *http.Client, endpoint, token string, index int) sample {
	s := sample{Index: index}
	start := time.Now()
	var mu sync.Mutex
	var conn, dns, handshake, first time.Time
	tcp := map[string]time.Time{}
	ms := func(d time.Duration) float64 { return float64(d) / float64(time.Millisecond) }
	trace := &httptrace.ClientTrace{
		GetConn: func(string) { mu.Lock(); conn = time.Now(); mu.Unlock() },
		GotConn: func(info httptrace.GotConnInfo) {
			mu.Lock()
			s.ConnectionMS = ms(time.Since(conn))
			s.Reused = info.Reused
			mu.Unlock()
		},
		DNSStart:     func(httptrace.DNSStartInfo) { mu.Lock(); dns = time.Now(); mu.Unlock() },
		DNSDone:      func(httptrace.DNSDoneInfo) { mu.Lock(); s.DNSMS += ms(time.Since(dns)); mu.Unlock() },
		ConnectStart: func(network, address string) { mu.Lock(); tcp[network+"\x00"+address] = time.Now(); mu.Unlock() },
		ConnectDone: func(network, address string, _ error) {
			mu.Lock()
			if since, ok := tcp[network+"\x00"+address]; ok {
				s.ConnectMS += ms(time.Since(since))
			}
			mu.Unlock()
		},
		TLSHandshakeStart:    func() { mu.Lock(); handshake = time.Now(); mu.Unlock() },
		TLSHandshakeDone:     func(tls.ConnectionState, error) { mu.Lock(); s.TLSMS += ms(time.Since(handshake)); mu.Unlock() },
		GotFirstResponseByte: func() { mu.Lock(); first = time.Now(); s.TTFBMS = ms(first.Sub(start)); mu.Unlock() },
	}
	// A single read-only statement and protocol close share one HTTP request.
	body := []byte(`{"requests":[{"type":"execute","stmt":{"sql":"SELECT 1","args":[],"want_rows":true}},{"type":"close"}]}`)
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), http.MethodPost, endpoint, bytes.NewReader(body))
	if err != nil {
		s.Class = "request_setup"
		return s
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	response, err := client.Do(req)
	if err != nil {
		s.Class = errorClass(err)
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
	} else {
		s.Status = response.StatusCode
		data, readErr := io.ReadAll(io.LimitReader(response.Body, (1<<20)+1))
		closeErr := response.Body.Close()
		if readErr != nil || closeErr != nil {
			s.Class = "body_read"
		} else if len(data) > 1<<20 {
			s.Class = "response_limit"
		} else if response.StatusCode != http.StatusOK {
			s.Class = "http_error"
		} else {
			var decoded struct {
				Results []struct {
					Type     string `json:"type"`
					Response struct {
						Type   string `json:"type"`
						Result struct {
							Rows [][]struct{ Type, Value string } `json:"rows"`
						} `json:"result"`
					} `json:"response"`
				} `json:"results"`
			}
			if json.Unmarshal(data, &decoded) == nil && len(decoded.Results) == 2 &&
				decoded.Results[0].Type == "ok" && decoded.Results[1].Type == "ok" &&
				decoded.Results[0].Response.Type == "execute" && decoded.Results[1].Response.Type == "close" {
				rows := decoded.Results[0].Response.Result.Rows
				s.ResultOne = len(rows) == 1 && len(rows[0]) == 1 && rows[0][0].Type == "integer" && rows[0][0].Value == "1"
			}
			if s.ResultOne {
				s.Class = "ok"
			} else {
				s.Class = "protocol_or_sql_error"
			}
		}
	}
	mu.Lock()
	s.ElapsedMS = ms(time.Since(start))
	if !first.IsZero() {
		s.BodyMS = ms(time.Since(first))
	}
	result := s
	mu.Unlock()
	return result
}

func run(backend, out string, count int) bool {
	prefix := "MQLITE_DIAG_" + strings.ToUpper(backend)
	dsn, token := os.Getenv(prefix+"_DB"), os.Getenv(prefix+"_TOKEN")
	sum := sha256.Sum256([]byte(dsn))
	if approved[backend] == "" || hex.EncodeToString(sum[:]) != approved[backend] || token == "" {
		return false
	}
	u, err := url.Parse(dsn)
	if err != nil || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return false
	}
	u.Scheme, u.Path = "https", "/v2/pipeline"
	transport := http.DefaultTransport.(*http.Transport).Clone()
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	programHash := sha256.Sum256(programSource)
	r := report{Scope: "read_only_protocol_control_not_engine_or_capacity_acceptance", Backend: backend, ProgramSHA: hex.EncodeToString(programHash[:]),
		Go: runtime.Version(), OS: runtime.GOOS, Arch: runtime.GOARCH, StartedAt: time.Now().UTC().Format(time.RFC3339Nano), Passed: true,
		Limitations: []string{"TTFB includes network and service processing; it is not pure network RTT.", "Fresh connection timings include routing/edge effects and do not isolate database execution.", "No MQLite engine, schema reset, data write, callback, or BEGIN CONCURRENT is used."}}
	for _, name := range []string{"HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"} {
		if os.Getenv(name) != "" {
			r.ProxySet = true
		}
	}
	for i := 0; i < count; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		s := request(ctx, client, u.String(), token, i)
		cancel()
		r.Samples = append(r.Samples, s)
		if !s.ResultOne {
			r.Passed = false
			break
		}
	}
	r.FinishedAt = time.Now().UTC().Format(time.RFC3339Nano)
	encoded, err := json.MarshalIndent(r, "", "  ")
	if err != nil || os.MkdirAll(out, 0700) != nil {
		return false
	}
	if os.WriteFile(filepath.Join(out, backend+"-protocol-baseline.json"), append(encoded, '\n'), 0600) != nil {
		return false
	}
	return r.Passed
}

func main() {
	backend := flag.String("backend", "both", "standard, rust or both")
	out := flag.String("out", "", "public evidence directory")
	count := flag.Int("count", 10, "fixed read-only sample count")
	stdin := flag.Bool("credentials-stdin", false, "read permitted diagnostic environment values from stdin")
	flag.Parse()
	if *out == "" || *count < 1 || *count > 20 || (*backend != "both" && *backend != "standard" && *backend != "rust") {
		os.Exit(2)
	}
	if _, ok := os.LookupEnv("MQLITE_TEST_DB"); ok {
		os.Exit(2)
	}
	if _, ok := os.LookupEnv("MQLITE_TEST_DB_RESET_CONFIRM"); ok {
		os.Exit(2)
	}
	if *stdin {
		scanner := bufio.NewScanner(os.Stdin)
		seen := map[string]bool{}
		for scanner.Scan() {
			name, value, ok := strings.Cut(scanner.Text(), "=")
			if !ok || seen[name] || (name != "MQLITE_DIAG_STANDARD_DB" && name != "MQLITE_DIAG_STANDARD_TOKEN" && name != "MQLITE_DIAG_RUST_DB" && name != "MQLITE_DIAG_RUST_TOKEN") {
				os.Exit(2)
			}
			seen[name] = true
			_ = os.Setenv(name, value)
		}
		if scanner.Err() != nil {
			os.Exit(2)
		}
	}
	backends := []string{*backend}
	if *backend == "both" {
		backends = []string{"standard", "rust"}
	}
	for _, b := range backends {
		if !run(b, *out, *count) {
			os.Exit(1)
		}
	}
}
