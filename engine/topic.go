package engine

import "strings"

// Filter is a subscription filter: equality-AND over properties plus an optional
// subject prefix (Correlation-style, §10.2). Evaluated in Go at publish time.
type Filter struct {
	Equals        map[string]string `json:"equals,omitempty"`
	SubjectPrefix string            `json:"subject_prefix,omitempty"`
}

func (f *Filter) match(m OutMessage) bool {
	if f == nil {
		return true
	}
	if f.SubjectPrefix != "" && !strings.HasPrefix(m.Subject, f.SubjectPrefix) {
		return false
	}
	for k, v := range f.Equals {
		if m.Properties[k] != v {
			return false
		}
	}
	return true
}
