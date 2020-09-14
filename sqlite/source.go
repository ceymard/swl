package swlite

import (
	"fmt"

	"github.com/ceymard/swl/swllib"

	"database/sql"

	// need sqlite, can be disabled in build
	_ "github.com/mattn/go-sqlite3"
)

func init() {
	swllib.RegisterSource(sqliteSourceCreator, "sqlite handler", "sqlite",
		".sqlite",
		".sqlite3",
		".db",
		"sqlite://",
	)
}

type sqliteSourceArgs struct {
	URI     string   `arg required type:'path'`
	Sources []string `arg optional`
	Coerce  bool
}

// The SQLite source for swl
func sqliteSourceCreator(pipe *swllib.Pipe, args []string) (swllib.Source, error) {
	var (
		err error
		db  *sql.DB
		req []swllib.SQLTableRequest
	)

	cli := sqliteSourceArgs{}
	if err = swllib.ParseArgs(&cli, args); err != nil {
		return nil, err
	}

	// pp.Print(cli)
	// journal mode off only for source, force wal otherwise.
	if db, err = sql.Open("sqlite3", "file:"+cli.URI+"?mode=ro"); err != nil {
		return nil, err
	}

	if len(cli.Sources) > 0 {
		req = swllib.ParseSQLTableRequests(cli.Sources)
	}

	return &sqliteSource{pipe, db, req}, nil
}

type sqliteSource struct {
	pipe   *swllib.Pipe
	conn   *sql.DB
	tables []swllib.SQLTableRequest
}

// Returns a list of table or views to be queried by default
// when no table argument is provided
func (s *sqliteSource) getAllTableLikes() error {
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
		s.tables = append(s.tables, swllib.SQLTableRequest{
			Colname: table,
			Query:   fmt.Sprintf(`SELECT * FROM '%s'`, table),
		})
	}

	return nil
}

func (s *sqliteSource) Emit() error {
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

func (s *sqliteSource) processTable(tbl swllib.SQLTableRequest) error {
	var (
		rows  *sql.Rows
		cols  []string
		types []*sql.ColumnType
		err   error
		m     map[string]interface{}
	)

	if rows, err = s.conn.Query(tbl.Query); err != nil {
		return fmt.Errorf(`In query [%s], %w`, tbl.Query, err)
	}
	defer rows.Close()

	if cols, err = rows.Columns(); err != nil {
		return err
	}
	if types, err = rows.ColumnTypes(); err != nil {
		return err
	}

	// FIXME : for columns that come from views or dynamic statements,
	// we might need to process the first rows that are not null to get
	// correct typing...
	var hints = make(map[string]string)
	for i, col := range cols {
		hints[col] = types[i].DatabaseTypeName()
	}

	s.pipe.WriteStartCollection(&swllib.CollectionStartChunk{
		Name:      tbl.Colname,
		TypeHints: hints,
	})
	for rows.Next() {
		if m, err = swllib.SqlRowsToMap(cols, rows); err != nil {
			return err
		}
		// Outputs: map[columnName:value columnName2:value2 columnName3:value3 ...]
		if err = s.pipe.WriteData(m); err != nil {
			return err
		}
	}

	return err
}
