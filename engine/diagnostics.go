package engine

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

// ErrorDetail is safe for monitor credentials. Message is a generated description,
// never a provider response, SQL statement, URL, credential or message body.
// Fingerprint correlates it with the private operator log without exposing the text.
type ErrorDetail struct {
	AtMs        int64  `json:"at_ms"`
	Category    string `json:"category"`
	Message     string `json:"message"`
	Fingerprint string `json:"fingerprint"`
	HTTPStatus  int    `json:"http_status"`
	SQLiteCode  int    `json:"sqlite_code"`
}

type MaintenanceFailure struct {
	Stage string      `json:"stage"`
	Error ErrorDetail `json:"error"`
}

// MaintenanceDiagnostics is an optional extension: absent means an older broker,
// not a healthy task. History is bounded to eight failed stages per task/process.
type MaintenanceDiagnostics struct {
	LastOutcome         string               `json:"last_outcome"`
	LastRunAtMs         int64                `json:"last_run_at_ms"`
	ConsecutiveFailures uint64               `json:"consecutive_failures"`
	RecoveredAtMs       int64                `json:"recovered_at_ms"`
	RecentFailures      []MaintenanceFailure `json:"recent_failures"`
}

const maintenanceHistoryLimit = 8

var providerHTTPCode = regexp.MustCompile(`^error code ([1-5][0-9]{2}):`)

// singleRemoteProviderLeaf follows only the pinned driver's single-cause
// wrappers. Joined errors, network wrappers and overly deep chains are not
// evidence of one provider response.
func singleRemoteProviderLeaf(err error) error {
	for depth := 0; err != nil && depth < 32; depth++ {
		if _, multiple := err.(interface{ Unwrap() []error }); multiple {
			return nil
		}
		//nolint:errorlint // Inspect this node only; errors.As can invoke custom As or traverse joined/cyclic causes.
		if _, transport := err.(net.Error); transport {
			return nil
		}
		cause := errors.Unwrap(err)
		if cause == nil {
			return err
		}
		err = cause
	}
	return nil
}

// remoteProviderHTTPStatus recognizes the pinned driver's HTTP response leaf,
// never matching SQL or explanations in outer wrappers.
func remoteProviderHTTPStatus(err error) int {
	if leaf := singleRemoteProviderLeaf(err); leaf != nil {
		if match := providerHTTPCode.FindStringSubmatch(leaf.Error()); len(match) == 2 {
			status, _ := strconv.Atoi(match[1])
			return status
		}
	}
	return 0
}

func transientProviderHTTPStatus(status int) bool {
	return status == 408 || status == 429 || status == 499 || status >= 500 && status <= 599
}

// The pinned driver's fallback JSON decoder can discard the HTTP status and
// return "error code SQLITE_BUSY" or wrap ErrBadConn under STREAM_EXPIRED.
// Neither proves a write was rejected. This guard only refuses unsafe replay;
// it does not invent the missing status or permit a read retry.
func unclassifiedProviderHTTPCode(err error) bool {
	if singleRemoteProviderLeaf(err) == nil || remoteProviderHTTPStatus(err) != 0 {
		return false
	}
	for depth := 0; err != nil && depth < 32; depth++ {
		//nolint:errorlint // Recognize only this node's marker; errors.As would bypass this bounded single-cause walk.
		if _, opaque := err.(*providerHTTPCodeFailure); opaque {
			return true
		}
		if strings.HasPrefix(err.Error(), "error code ") {
			return true
		}
		err = errors.Unwrap(err)
	}
	return false
}

func describeError(err error, now int64) (d ErrorDetail) {
	d = ErrorDetail{AtMs: now, Category: storageErrorCode(err)}
	// A specific transport cause must not obscure ambiguous commit semantics.
	defer func() {
		if errors.Is(err, ErrOutcomeUnknown) {
			d.Category = "outcome_unknown"
			d.Message = "Write acknowledgement was lost; the commit outcome must be reconciled before retrying."
		}
	}()
	raw := err.Error()
	hash := sha256.Sum256([]byte(raw))
	d.Fingerprint = hex.EncodeToString(hash[:12])
	if isExpiredRemoteTransactionErr(err) {
		// Precision belongs in the optional detail, not a new required storage-counter
		// tuple that would make current clients reject older broker observations.
		d.Category = "transaction_expired"
		d.Message = "Remote database rolled back an idle transaction; the permitted retries were exhausted."
		return d
	}
	if status := remoteProviderHTTPStatus(err); status != 0 {
		d.HTTPStatus = status
		switch {
		case d.HTTPStatus == 401 || d.HTTPStatus == 403:
			d.Category = "provider_auth"
		case d.HTTPStatus == 429:
			d.Category = "provider_rate_limit"
		case d.HTTPStatus >= 500:
			d.Category = "provider_unavailable"
		default:
			d.Category = "provider_http"
		}
		d.Message = fmt.Sprintf("Database provider returned HTTP %d (%s).", d.HTTPStatus, http.StatusText(d.HTTPStatus))
		return d
	}
	var dns *net.DNSError
	var timeout net.Error
	var cert *tls.CertificateVerificationError
	var unknownCA x509.UnknownAuthorityError
	var syntax *json.SyntaxError
	var mismatch *json.UnmarshalTypeError
	switch {
	case errors.As(err, &dns):
		d.Category, d.Message = "dns", "Database hostname lookup failed."
	case errors.As(err, &cert), errors.As(err, &unknownCA):
		d.Category, d.Message = "tls", "Database TLS certificate verification failed."
	case errors.Is(err, context.DeadlineExceeded), errors.As(err, &timeout) && timeout.Timeout():
		d.Category, d.Message = "timeout", "Database operation exceeded its deadline or transport timeout."
	case errors.As(err, &syntax), errors.As(err, &mismatch):
		d.Category, d.Message = "protocol", "Database provider returned an invalid JSON protocol response."
	}
	if d.Message != "" {
		return d
	}
	var coded interface{ Code() int }
	if errors.As(err, &coded) {
		d.SQLiteCode = coded.Code()
	} else if isRustTursoBusyErr(err) {
		// The pinned HTTP driver drops the Rust provider's numeric result code.
		// Its exact Busy variant is SQLITE_BUSY, not an ambiguous transport error.
		d.SQLiteCode = 5
	}
	upper := strings.ToUpper(raw)
	switch {
	case d.SQLiteCode&255 == 19 || strings.Contains(upper, "SQLITE_CONSTRAINT"):
		d.Category, d.Message = "constraint", "Database constraint rejected the operation."
	case d.SQLiteCode&255 == 8 || strings.Contains(upper, "SQLITE_READONLY"):
		d.Category, d.Message = "readonly", "Database rejected a write because it is read-only."
	case strings.Contains(upper, "NO SUCH TABLE"), strings.Contains(upper, "NO SUCH COLUMN"), d.SQLiteCode&255 == 17:
		d.Category, d.Message = "schema", "Database schema or required table/column is unavailable."
	default:
		messages := map[string]string{
			"outcome_unknown": "Write acknowledgement was lost; the commit outcome must be reconciled before retrying.",
			"canceled":        "Database operation was canceled.", "closed": "Database connection pool was closed.",
			"busy":       "Database lock contention exhausted the permitted retries.",
			"connection": "Database connection or transport failed.",
			"full":       "Database storage is full.", "corrupt": "Database is corrupt or has an invalid format.",
			"io": "Database storage I/O failed.", "application": "The operation was rejected by an application rule.",
		}
		d.Message = messages[d.Category]
		if d.Message == "" {
			d.Category = "unclassified"
			d.Message = "Unrecognized database error; correlate the fingerprint with the private diagnostic log."
		}
	}
	return d
}

// Private logs preserve the remaining provider explanation, but redact common
// secret formats and entire URLs. This is defense in depth, not a public-data
// sanitizer: operator log files must remain private, including unknown SQL errors.
var diagnosticURL = regexp.MustCompile(`(?i)(?:https?|libsql|wss?|file)://[^\s"'<>]+`)
var diagnosticCredential = regexp.MustCompile(`(?i)(?:bearer\s+|(?:authorization|auth[_-]?token|token|password|secret|api[_-]?key)\s*[:=]\s*)[^\s,;]+`)
var diagnosticToken = regexp.MustCompile(`mqk_[a-zA-Z0-9_-]+|eyJ[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+\.[a-zA-Z0-9_-]+`)

func privateDiagnosticError(err error) string {
	text := diagnosticURL.ReplaceAllString(err.Error(), "[redacted-url]")
	text = diagnosticCredential.ReplaceAllString(text, "[redacted-credential]")
	text = diagnosticToken.ReplaceAllString(text, "[redacted-token]")
	// Bound records and prevent multiline terminal/log injection.
	text = strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, text)
	text = strings.Join(strings.Fields(text), " ")
	if len(text) > 2048 {
		text = strings.ToValidUTF8(text[:2048], "") + " [truncated]"
	}
	return text
}

func (e *Engine) maintenanceFailure(ctx context.Context, stage string, err error) {
	markMaintenanceFailure(ctx, err)
	detail := describeError(err, e.now())
	task := "manual"
	if a, ok := ctx.Value(maintenanceContextKey{}).(*maintenanceAttempt); ok {
		task = a.task
		if !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, ErrClosed) {
			a.failures = append(a.failures, MaintenanceFailure{Stage: stage, Error: detail})
			if len(a.failures) > maintenanceHistoryLimit {
				a.failures = a.failures[1:]
			}
		}
	}
	if e.log == nil {
		return
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, ErrClosed) {
		e.log.Info("maintenance.interrupted", "task", task, "stage", stage, "at_ms", detail.AtMs, "category", detail.Category)
		return
	}
	e.log.Error("maintenance.failure", "task", task, "stage", stage, "at_ms", detail.AtMs,
		"category", detail.Category, "cause", detail.Message, "fingerprint", detail.Fingerprint,
		"http_status", detail.HTTPStatus, "sqlite_code", detail.SQLiteCode, "error", privateDiagnosticError(err))
}
