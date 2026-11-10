package hranaV2

import "errors"

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
