package engine

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"github.com/mqlitehq/mqlite/internal/libsqlcompat"
	"net"
)

func isRustTursoBusyErr(err error) bool {
	return exactRemoteProviderError(err, "Tursodb error: Database is busy")
}

func exactRemoteProviderError(err error, message string) bool {
	if err == nil || errors.Is(err, ErrOutcomeUnknown) ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || isConnErr(err) {
		return false
	}
	var transport net.Error
	if errors.As(err, &transport) {
		return false
	}
	// Only the driver's single-cause wrapping is evidence of a provider reply.
	// A joined transport/cancellation failure is not a definite rejection.
	for depth := 0; depth < 32; depth++ {
		if _, multiple := err.(interface{ Unwrap() []error }); multiple {
			return false
		}
		cause := errors.Unwrap(err)
		if cause == nil {
			return err.Error() == message
		}
		err = cause
	}
	return false
}

func isExpiredRemoteTransactionErr(err error) bool {
	return exactRemoteProviderError(err, "SQLite error: interactive transaction was rolled back "+
		"because the stream was idle for too long; retry the transaction")
}

type providerHTTPCodeFailure struct{ cause error }

func (e *providerHTTPCodeFailure) Error() string { return e.cause.Error() }

func isolateProviderBadConn(err error) error {
	if errors.Is(err, driver.ErrBadConn) && (remoteProviderHTTPStatus(err) != 0 || unclassifiedProviderHTTPCode(err)) {
		return &providerHTTPCodeFailure{cause: err}
	}
	return err
}

// Cancellation after SQL dispatch cannot prove the absence of an effect.
func remoteWriteMayHaveCommitted(err error) bool {
	return libsqlcompat.IsInvalidAcknowledgement(err) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || isConnErr(err) || unclassifiedProviderHTTPCode(err) || isBusyErr(err) || isExpiredRemoteTransactionErr(err)
}

type providerWriteOutcomeUnknown struct{ cause error }

func (e *providerWriteOutcomeUnknown) Error() string {
	return fmt.Sprintf("%v: %v", ErrOutcomeUnknown, e.cause)
}

func (e *providerWriteOutcomeUnknown) Unwrap() error { return e.cause }

func (e *providerWriteOutcomeUnknown) Is(target error) bool { return target == ErrOutcomeUnknown }

func (e *providerWriteOutcomeUnknown) HTTPStatus() int { return remoteProviderHTTPStatus(e.cause) }

func unknownRemoteWrite(err error) error {
	if remoteProviderHTTPStatus(err) != 0 {
		return &providerWriteOutcomeUnknown{cause: isolateProviderBadConn(err)}
	}
	if unclassifiedProviderHTTPCode(err) {
		return &providerWriteOutcomeUnknown{cause: isolateProviderBadConn(err)}
	}
	if isBusyErr(err) || isExpiredRemoteTransactionErr(err) {
		return &providerWriteOutcomeUnknown{cause: err}
	}
	// Preserve the established broad-transport contract: EOF/reset is not a
	// safe-retry sentinel once an issued write's acknowledgement is lost.
	return fmt.Errorf("%w: %s", ErrOutcomeUnknown, err.Error())
}
