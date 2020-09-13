package swlite

import (
	"fmt"
	"strings"

	"github.com/ceymard/swl/swllib"

	"database/sql"

	"github.com/alecthomas/kong"
	_ "github.com/mattn/go-sqlite3"
)

type SqliteSourceArgs struct {
	URI     string   `arg required type:'path'`
	Sources []string `arg optional`
	Coerce  bool
}

// The SQLite source for swl
func SqliteSourceCreator(pipe *swllib.Pipe, args []string) (swllib.Source, error) {
	var (
		err    error
		parser *kong.Kong
		tbl    = make([]TableRequest, 0, 24)
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
	// journal mode off only for source, force wal otherwise.
	if db, err = sql.Open("sqlite3", cli.URI+"?_mode=ro"); err != nil {
		return nil, err
	}

	if len(cli.Sources) > 0 {
		for _, s := range cli.Sources {
			var (
				query string
			)
			query = s
			if strings.Contains(s, ":") {
				var parts = strings.SplitN(s, ":", 2)
				s = parts[0]
				query = parts[1]
				if strings.Index(strings.ToLower(strings.TrimSpace(query)), "select") != 0 {
					query = fmt.Sprintf(`SELECT * FROM "%s"`, query)
				}
			} else {
				query = fmt.Sprintf(`SELECT * FROM "%s"`, query)
			}

			tbl = append(tbl, TableRequest{
				name:  s,
				query: query,
			})
		}
	}

	return &SqliteSource{pipe, db, tbl}, nil
}

type SqliteSource struct {
	pipe   *swllib.Pipe
	conn   *sql.DB
	tables []TableRequest
}

type TableRequest struct {
	name  string
	query string
}

// Returns a list of table or views to be queried by default
// when no table argument is provided
func (s *SqliteSource) getAllTableLikes() error {
	var (
		table string
		err   error
		rows  *sql.Rows
	)

	if rows, err = s.conn.Query(`SELECT name FROM sqlite_master WHERE type = 'table'`); err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		if err = rows.Scan(&table); err != nil {
			return err
		}
		s.tables = append(s.tables, TableRequest{
			name:  table,
			query: fmt.Sprintf(`SELECT * FROM '%s'`, table),
		})
	}

	return nil
}

func (s *SqliteSource) Emit() error {
	defer s.conn.Close()

	var (
		err error
	)

	// If no tables where requested in particular, we just output everything
	if len(s.tables) == 0 {
		if err = s.getAllTableLikes(); err != nil {
			return err
		}
	}

	for _, tbl := range s.tables {
		if err = s.processTable(tbl); err != nil {
			return err
		}
	}

	return nil
}

func (s *SqliteSource) processTable(tbl TableRequest) error {
	var (
		rows *sql.Rows
		cols []string
		err  error
		m    map[string]interface{}
	)

	if rows, err = s.conn.Query(tbl.query); err != nil {
		return fmt.Errorf(`In query [%s], %w`, tbl.query, err)
	}
	defer rows.Close()

	if cols, err = rows.Columns(); err != nil {
		return err
	}

	s.pipe.WriteStartCollection(tbl.name)
	for rows.Next() {
		if m, err = RowsToMap(cols, rows); err != nil {
			return err
		}
		// Outputs: map[columnName:value columnName2:value2 columnName3:value3 ...]
		s.pipe.WriteData(m)
	}

	return err
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
