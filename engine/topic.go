package engine

// Filter is a subscription filter: a single expr-lang boolean predicate over a
// message. An empty Expr matches every message. The expression is type-checked and
// compiled once at Subscribe (a bad one is rejected with ErrInvalidFilter) and run
// per message at publish (fan-out). The language surface — the variables a filter
// sees, the helpers, and the fail-closed evaluator — lives in filter.go.
//
// This replaces the earlier equality-AND + subject-prefix form: a filter is now just
// `subject_parts[0] == "orders" && properties["tier"] == "gold"` (MQLITE-17).
type Filter struct {
	Expr string `json:"expr,omitempty"`
}
