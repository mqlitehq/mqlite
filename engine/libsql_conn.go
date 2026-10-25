package engine

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"

	libsql "github.com/tursodatabase/libsql-client-go/libsql"
)

// openHTTPHranaDB opens the documented libsql:// transport through a conservative
// compatibility adapter for libsql-client-go's HTTP v2 connection lifecycle
// (MQLITE-163).
//
// A successful Hrana response may carry no baton, which means the server closed
// that logical stream. The deprecated driver records that fact internally but
// does not implement driver.Validator, so database/sql can otherwise return the
// known-closed connection to an idle slot or directly to a waiting caller. The
// next operation then fails locally with driver.ErrBadConn before sending HTTP.
//
// Treat each driver connection as one database/sql lease (a whole transaction is
// one lease). Validator runs only when that lease is returned, so it never breaks
// a transaction in progress; returning false prevents both idle reuse and direct
// waiter handoff. The driver's shared http.DefaultTransport still reuses the
// underlying TCP/TLS connection. Remove this adapter when MQLite can move to a
// maintained pure-Go remote driver with an equivalent validity contract.
func openHTTPHranaDB(dsn string) (*sql.DB, error) {
	// Use the driver's sql.Open-compatible DSN parser so this adapter preserves
	// the existing authToken/tls URL contract exactly. NewConnector intentionally
	// rejects those legacy query parameters, while resolveDSN still emits them.
	// Re-parsing the URL here would risk changing credential or TLS behavior.
	base := libsqlDSNConnector{dsn: dsn}
	return sql.OpenDB(singleLeaseConnector{Connector: base}), nil
}

// libsqlDSNConnector is the driver.Connector equivalent of sql.Open("libsql",
// dsn). The upstream Driver does not implement driver.DriverContext, so this is
// the narrow bridge needed to place the validity adapter around each connection.
type libsqlDSNConnector struct {
	dsn string
}

func (c libsqlDSNConnector) Connect(ctx context.Context) (driver.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return (libsql.Driver{}).Open(c.dsn)
}

func (libsqlDSNConnector) Driver() driver.Driver { return libsql.Driver{} }

type singleLeaseConnector struct {
	driver.Connector
}

func (c singleLeaseConnector) Connect(ctx context.Context) (driver.Conn, error) {
	conn, err := c.Connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	httpConn, ok := conn.(httpHranaConn)
	if !ok {
		_ = conn.Close()
		return nil, fmt.Errorf("libsql HTTP connection does not provide the required context interfaces")
	}
	return &singleLeaseConn{httpHranaConn: httpConn}, nil
}

// singleLeaseConn deliberately reports itself invalid when database/sql returns
// it to the pool. Requiring and embedding the current driver's context-aware
// interface set preserves query, transaction, and cancellation behavior without
// falling back to the deprecated driver.Conn.Begin method.
type httpHranaConn interface {
	driver.Conn
	driver.ConnPrepareContext
	driver.ConnBeginTx
	driver.ExecerContext
	driver.QueryerContext
}

type singleLeaseConn struct {
	httpHranaConn
}

func (*singleLeaseConn) IsValid() bool { return false }

var (
	_ driver.Connector          = libsqlDSNConnector{}
	_ driver.Connector          = singleLeaseConnector{}
	_ driver.Validator          = (*singleLeaseConn)(nil)
	_ driver.ConnPrepareContext = (*singleLeaseConn)(nil)
	_ driver.ConnBeginTx        = (*singleLeaseConn)(nil)
	_ driver.ExecerContext      = (*singleLeaseConn)(nil)
	_ driver.QueryerContext     = (*singleLeaseConn)(nil)
)
