package pg

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/ceymard/swl/swllib"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
)

func init() {
	swllib.RegisterSource(PostgresSourceCreator, `postgres source`,
		"pg",
		"postgres://",
	)
}

type PostgresSourceArgs struct {
	URI     string   `arg required`
	Schema  string   `optional`
	Sources []string `arg optional`
}

func PostgresSourceCreator(pipe *swllib.Pipe, args []string) (swllib.Source, error) {
	var (
		arr PostgresSourceArgs
		err error
	)

	if err = swllib.ParseArgs(&arr, args); err != nil {
		return nil, err
	}

	return &PostgresSource{arr, pipe}, nil
}

type PostgresSource struct {
	args PostgresSourceArgs
	pipe *swllib.Pipe
}

func (ps *PostgresSource) Emit() error {

	var (
		req  []swllib.SQLTableRequest
		ctx  = context.Background()
		conn *pgx.Conn
		uri  *swllib.TunneledURI
		err  error
	)

	if uri, err = swllib.HandleURI(ps.args.URI); err != nil {
		return err
	}

	defer uri.Close()
	// Close the SSH tunnel if there was one.

	url := uri.ClientURI
	if !strings.Contains(uri.ClientURI, "postgres://") {
		url = "postgres://" + url
	}

	if conn, err = pgx.Connect(ctx, url); err != nil {
		return err
	}

	defer func() {
		// unfortunately, it seems that closing inside a ssh tunnel just doesn't work when a tunnel is opened,
		// as it waits for the tunnel to be closed (?!)
		go conn.Close(ctx)
	}()

	if len(ps.args.Sources) > 0 {
		req = swllib.ParseSQLTableRequests(ps.args.Sources)
	} else {
		if req, err = ps.ComputeAllTables(ctx, conn); err != nil {
			return err
		}
	}

	for _, r := range req {
		if err = ps.HandleCollection(ctx, conn, r); err != nil {
			return err
		}
	}

	return nil
}

var reNoTransform = regexp.MustCompile(`^(?i)(text|int|float|bool|byte|varchar|char|json|date)`)

func (ps *PostgresSource) HandleCollection(ctx context.Context, client *pgx.Conn, req swllib.SQLTableRequest) error {
	var (
		rows      pgx.Rows
		cols      []string
		typehints = make(map[string]string)
		err       error
		stmt      *pgconn.StatementDescription
	)

	var stmtName = "tmp-stmt-will-be-deleted"

	// First we create a statement, which we will use to get the type hints.
	if stmt, err = client.Prepare(ctx, stmtName, req.Query); err != nil {
		return fmt.Errorf(`could not create statement for '%s' %w`, req.Query, err)
	}

	var desc = stmt.Fields
	var json_forced_cols = make([]bool, len(desc))
	cols = make([]string, len(desc))
	for i, d := range desc {
		cols[i] = string(d.Name)
		// FIXME, we should get the types sometime before...
		var tname, ok = client.ConnInfo().DataTypeForOID(desc[i].DataTypeOID)
		if !ok {
			typehints[cols[i]] = "JSON"
			json_forced_cols[i] = true
			// return fmt.Errorf(`unknown data type OID '%v' for column '%s'`, desc[i].DataTypeOID, d.Name)
		} else {
			var name = strings.ToUpper(tname.Name)
			if !reNoTransform.MatchString(name) {
				json_forced_cols[i] = true
				typehints[cols[i]] = "JSON"
			} else {
				typehints[cols[i]] = name
			}
		}
	}

	// Release the statement, as we're going to handle the rows as json instead
	if err = client.Deallocate(ctx, stmtName); err != nil {
		return err
	}

	// wrap the query into some beautiful json stuff
	var builder swllib.Buffer
	builder.WriteStrings(`SELECT `)

	for i := range cols {
		if i > 0 {
			builder.WriteString(`, `)
		}
		if json_forced_cols[i] {
			builder.WriteStrings(`row_to_json(R)->'`, cols[i], `' as "`, cols[i], `"`)
		} else {
			builder.WriteStrings(cols[i])
		}
	}

	builder.WriteStrings(` FROM (`, req.Query, `) R`)

	if rows, err = client.Query(ctx, builder.String()); err != nil {
		return err
	}
	defer rows.Close()

	ps.pipe.WriteStartCollection(&swllib.CollectionStartChunk{
		Name:      req.Colname,
		TypeHints: typehints,
	})

	for rows.Next() {
		// var value interface{}

		// if err = rows.Scan(&value); err != nil {
		// 	return err
		// }
		if err = ps.handleRow(cols, rows); err != nil {
			return err
		}

		// if err = ps.pipe.WriteData(value.(map[string]interface{})); err != nil {
		// 	return err
		// }
	}

	return nil
}

func (ps *PostgresSource) handleRow(cols []string, rows pgx.Rows) error {
	var (
		values = make([]interface{}, len(cols))
		ptrs   = make([]interface{}, len(cols))
		dt     = make(map[string]interface{})
		err    error
	)

	for i := range cols {
		ptrs[i] = &values[i]
		// dt[name] = values[i]
	}
	if err = rows.Scan(ptrs...); err != nil {
		return err
	}
	for i, name := range cols {
		// ptrs[i] = &values[i]
		dt[name] = values[i]
	}

	return ps.pipe.WriteData(dt)
}

func (ps *PostgresSource) ComputeAllTables(ctx context.Context, client *pgx.Conn) ([]swllib.SQLTableRequest, error) {
	return nil, nil
}
