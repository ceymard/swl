package swlite

import (
	"github.com/ceymard/swl/swllib"

	"database/sql"

	"github.com/alecthomas/kong"
	_ "github.com/mattn/go-sqlite3"
)

type SqliteSourceArgs struct {
	URI     string   `arg required type:'path'`
	Sources []string `arg`
	Coerce  bool
}

func SqliteSourceCreator(pipe *swllib.Pipe, args []string) (swllib.Source, error) {
	var (
		err    error
		parser *kong.Kong
		db     *sql.DB
	)

	cli := SqliteSourceArgs{}
	if parser, err = kong.New(&cli); err != nil {
		return nil, err
	}

	if _, err = parser.Parse(args); err != nil {
		return nil, err
	}

	// pp.Print(cli)
	if db, err = sql.Open("sqlite3", cli.URI); err != nil {
		return nil, err
	}

	return &SqliteSource{pipe, db}, nil
}

type SqliteSource struct {
	pipe *swllib.Pipe
	conn *sql.DB
}

type SqliteTables struct {
}

func (s *SqliteSource) Emit() error {
	defer s.pipe.Close()
	defer s.conn.Close()

	var (
		rows *sql.Rows
		cols []string
		err  error
		m    map[string]interface{}
	)

	if rows, err = s.conn.Query(`SELECT * FROM targets`); err != nil {
		return err
	}
	defer rows.Close()

	if cols, err = rows.Columns(); err != nil {
		return err
	}

	s.pipe.WriteStartCollection("targets")
	for rows.Next() {
		if m, err = RowsToMap(cols, rows); err != nil {
			return err
		}
		// Outputs: map[columnName:value columnName2:value2 columnName3:value3 ...]
		s.pipe.WriteData(m)
	}

	return nil
}

func RowsToMap(cols []string, rows *sql.Rows) (map[string]interface{}, error) {
	// Create a slice of interface{}'s to represent each column,
	// and a second slice to contain pointers to each item in the columns slice.
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for i, _ := range columns {
		columnPointers[i] = &columns[i]
	}

	// Scan the result into the column pointers...
	if err := rows.Scan(columnPointers...); err != nil {
		return nil, err
	}

	// Create our map, and retrieve the value for each column from the pointers slice,
	// storing it in the map with the name of the column as the key.
	m := make(map[string]interface{})
	for i, colName := range cols {
		val := columnPointers[i].(*interface{})
		m[colName] = *val
	}

	return m, nil
}
