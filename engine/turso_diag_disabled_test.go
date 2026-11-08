//go:build !mqlite_perf_diag

package engine

import (
	"context"
	"database/sql"
	"reflect"
	"testing"
)

func TestDiagnosticDisabledHasNoPublicAPI(t *testing.T) {
	for _, name := range []string{"DiagnosticPoolStats", "DiagnosticSetPoolLimit", "DiagnosticSelectOne"} {
		if _, ok := reflect.TypeOf((*Engine)(nil)).MethodByName(name); ok {
			t.Fatalf("production build exports %s", name)
		}
	}
	if reflect.TypeOf((*diagnosticRows)(nil)) != reflect.TypeOf((*sql.Rows)(nil)) {
		t.Fatal("production rows type changed")
	}
	ctx := context.Background()
	if diagnosticOperationContext(ctx) != ctx || diagnosticAttemptContext(ctx, 7) != ctx {
		t.Fatal("disabled hook replaced the original context")
	}
}
