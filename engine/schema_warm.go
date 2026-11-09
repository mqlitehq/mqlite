package engine

import (
	"context"
	"database/sql"
	"strings"
)

type warmSchemaObject struct {
	kind, table, definition string
}

// Derive the inventory from the statements actually used to create the schema.
// This recognizes package-owned CREATE headers, not arbitrary application SQL.
// An unfamiliar future statement simply leaves Open on its established path.
func warmSchemaObjects() (map[string]warmSchemaObject, []any, bool) {
	objects := make(map[string]warmSchemaObject, len(schemaStmts))
	names := make([]any, 0, len(schemaStmts))
	for _, statement := range schemaStmts {
		definition := normalizedSchemaSQL(statement)
		header := strings.Fields(definition)
		if len(header) < 3 || header[0] != "CREATE" {
			return nil, nil, false
		}
		kindAt := 1
		if header[kindAt] == "UNIQUE" {
			kindAt++
		}
		if len(header) <= kindAt+1 {
			return nil, nil, false
		}
		kind := strings.ToLower(header[kindAt])
		name := strings.ToLower(header[kindAt+1])
		table := name
		switch kind {
		case "table":
		case "index", "trigger":
			table = ""
			for i := kindAt + 2; i+1 < len(header); i++ {
				if header[i] == "ON" {
					table = strings.ToLower(header[i+1])
					break
				}
			}
			if table == "" {
				return nil, nil, false
			}
		default:
			return nil, nil, false
		}
		if _, duplicate := objects[name]; duplicate {
			return nil, nil, false
		}
		objects[name] = warmSchemaObject{kind: kind, table: table, definition: definition}
		names = append(names, name)
	}
	return objects, names, len(objects) > 0
}

// A current token is necessary but not sufficient for the warm path. Confirm
// every owned object in one bounded catalog read before omitting no-op DDL.
// Missing/different objects use the original CREATE path; read errors do not.
func (d *db) hasCompleteWarmSchema(ctx context.Context) (bool, error) {
	objects, names, understood := warmSchemaObjects()
	if !understood {
		return false, nil
	}
	seen := make(map[string]bool, len(objects))
	complete := true
	query := `SELECT type,name,tbl_name,sql FROM sqlite_master WHERE name COLLATE NOCASE IN (` +
		strings.TrimSuffix(strings.Repeat("?,", len(names)), ",") + `)`
	err := d.queryRows(ctx, query, func(rows *sql.Rows) error {
		for rows.Next() {
			var kind, name, table string
			var definition sql.NullString
			if err := rows.Scan(&kind, &name, &table, &definition); err != nil {
				return err
			}
			name = strings.ToLower(name)
			object, owned := objects[name]
			if !owned || seen[name] || kind != object.kind || !strings.EqualFold(table, object.table) || !definition.Valid {
				complete = false
				continue
			}
			seen[name] = true
			matches := normalizedSchemaSQL(definition.String) == object.definition
			for _, guard := range parentGuardSchemas {
				if name == guard.name {
					matches = matchesParentGuardDefinition(definition.String, guard.current)
					break
				}
			}
			if !matches {
				complete = false
			}
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return rows.Close()
	}, names...)
	return complete && len(seen) == len(objects), err
}
