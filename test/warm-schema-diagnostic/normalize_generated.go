// Generated from the exact a2f engine normalizer; only the return form exposes tokens for redaction.
package main

import "strings"

func schemaTokens(s string) []string {
	var tokens []string
	word := func(c byte) bool {
		return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '_' || c == '$'
	}
	for i := 0; i < len(s); {
		if strings.ContainsRune(" \t\r\n\f", rune(s[i])) {
			i++
			continue
		}
		if strings.HasPrefix(s[i:], "--") {
			for i < len(s) && s[i] != '\n' {
				i++
			}
			continue
		}
		if strings.HasPrefix(s[i:], "/*") {
			if end := strings.Index(s[i+2:], "*/"); end >= 0 {
				i += end + 4
				continue
			}
		}
		start := i
		if s[i] == '\'' || s[i] == '"' || s[i] == '`' || s[i] == '[' {
			end := s[i]
			if end == '[' {
				end = ']'
			}
			i++
			for i < len(s) {
				if s[i] == end {
					i++
					if end != ']' && i < len(s) && s[i] == end {
						i++
						continue
					}
					break
				}
				i++
			}
			tokens = append(tokens, s[start:i])
		} else if word(s[i]) {
			for i < len(s) && word(s[i]) {
				i++
			}
			tokens = append(tokens, strings.ToUpper(s[start:i]))
		} else {
			end := i + 1
			for _, operator := range []string{"->>", ">=", "<=", "!=", "<>", "==", "||", "<<", ">>", "->"} {
				if strings.HasPrefix(s[i:], operator) {
					end = i + len(operator)
					break
				}
			}
			tokens = append(tokens, s[i:end])
			i = end
		}
	}
	// SQLite usually omits this clause in its catalog; Rust can retain it.
	kindAt := 1
	if len(tokens) > kindAt && tokens[kindAt] == "UNIQUE" {
		kindAt++
	}
	if len(tokens) > kindAt+3 && tokens[0] == "CREATE" && (tokens[kindAt] == "TABLE" || tokens[kindAt] == "INDEX" || tokens[kindAt] == "TRIGGER") &&
		tokens[kindAt+1] == "IF" && tokens[kindAt+2] == "NOT" && tokens[kindAt+3] == "EXISTS" {
		tokens = append(tokens[:kindAt+1], tokens[kindAt+4:]...)
	}
	return tokens
}

func normalizedSchemaSQL(s string) string { return strings.Join(schemaTokens(s), " ") }
func matchesParentGuardDefinition(actual, canonical string) bool {
	normalized := normalizedSchemaSQL(actual)
	if normalized == normalizedSchemaSQL(canonical) {
		return true
	}
	alternate := strings.Replace(canonical, "NEW.name<>OLD.name", "NEW.name!=OLD.name", 1)
	return normalized == normalizedSchemaSQL(alternate)
}
