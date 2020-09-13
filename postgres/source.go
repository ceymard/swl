package pg

import (
	"context"
	"fmt"
	"strconv"

	"github.com/ceymard/swl/swllib"
	"github.com/jackc/pgx/v4"
)

func init() {
	swllib.RegisterSource("pg", `postgres source`, PostgresSourceCreator)
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
	// Close the SSH tunnel if there was one.
	defer uri.Close()

	if conn, err = pgx.Connect(ctx, "postgres://"+uri.ClientURI); err != nil {
		return err
	}
	defer conn.Close(ctx)

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

	return err
}

func (ps *PostgresSource) HandleCollection(ctx context.Context, client *pgx.Conn, req swllib.SQLTableRequest) error {
	var (
		rows      pgx.Rows
		cols      []string
		typehints = make(map[string]string)
		err       error
		values    []interface{}
	)

	if rows, err = client.Query(ctx, fmt.Sprintf(`SELECT row_to_json(T) as res FROM (%s) T`, req.Query)); err != nil {
		return err
	}
	defer rows.Close()

	var desc = rows.FieldDescriptions()
	cols = make([]string, len(desc))
	for i, d := range desc {
		cols[i] = string(d.Name)
		// FIXME, we should get the types sometime before...
		typehints[cols[i]] = "oid::" + strconv.FormatInt(int64(d.DataTypeOID), 10)
	}

	ps.pipe.WriteStartCollection(&swllib.CollectionStartChunk{
		Name:      req.Colname,
		TypeHints: typehints,
	})

	for rows.Next() {
		if values, err = rows.Values(); err != nil {
			return err
		}
		if err = ps.pipe.WriteData(values[0].(map[string]interface{})); err != nil {
			return err
		}
	}

	return nil
}

func (ps *PostgresSource) ComputeAllTables(ctx context.Context, client *pgx.Conn) ([]swllib.SQLTableRequest, error) {
	return nil, nil
}
