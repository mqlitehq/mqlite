package hranaV2

import (
	"errors"
	"github.com/mqlitehq/mqlite/internal/libsqlcompat/hrana"
)

// providerHTTPError preserves the actual response status even when an upstream
// error-body fallback discards it. It does not infer whether SQL took effect.
type providerHTTPError struct {
	status int
	cause  error
}

func (e *providerHTTPError) Error() string { return e.cause.Error() }
func (e *providerHTTPError) Unwrap() error { return e.cause }

// HTTPStatus returns a status observed by this driver, never one parsed from
// untrusted body text. Joined or excessively deep causes carry no such proof.
func HTTPStatus(err error) int {
	for depth := 0; err != nil && depth < 32; depth++ {
		//nolint:errorlint // Inspect this private node, not an arbitrary As method or joined cause.
		if reply, ok := err.(*providerHTTPError); ok {
			return reply.status
		}
		if _, multiple := err.(interface{ Unwrap() []error }); multiple {
			return 0
		}
		err = errors.Unwrap(err)
	}
	return 0
}

// A well-formed single execute error with SQLite's primary Busy code is a
// definite rejection, unlike Busy text in an HTTP failure or a batch outcome.
// Retain that evidence for the existing typed-SQLite retry policy.
type sqliteBusyRejection struct{ cause error }

func (e *sqliteBusyRejection) Error() string { return e.cause.Error() }
func (e *sqliteBusyRejection) Unwrap() error { return e.cause }
func (e *sqliteBusyRejection) Code() int     { return 5 }

func providerExecutionError(request *hrana.PipelineRequest, reply *hrana.PipelineResponse, failure hrana.StreamResult) error {
	cause := errors.New(failure.Error.Message)
	if len(request.Requests) == 1 && len(reply.Results) == 1 && request.Requests[0].Type == "execute" && request.Requests[0].Stmt != nil && failure.Type == "error" && failure.Response == nil && failure.Error.Code != nil && *failure.Error.Code == "SQLITE_BUSY" {
		return &sqliteBusyRejection{cause: cause}
	}
	return cause
}
