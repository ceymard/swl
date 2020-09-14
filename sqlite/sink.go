package swlite

import (
	"database/sql"
	"fmt"

	"github.com/ceymard/swl/swllib"
)

func init() {
	swllib.RegisterSink(sqliteSinkCreator, "sqlite",
		"sqlite",
		".db",
		".sqlite",
		".sqlite3",
	)
}

type sqliteSinkArgs struct {
	URI      string   `arg required type:'path'`
	Sources  []string `arg optional`
	Drop     bool
	Truncate bool
	Upsert   bool
}

func sqliteSinkCreator(pipe *swllib.Pipe, args []string) (swllib.Sink, error) {

	var (
		arg sqliteSinkArgs
		err error
		db  *sql.DB
	)

	if err = swllib.ParseArgs(&arg, args); err != nil {
		return nil, err
	}

	if db, err = sql.Open("sqlite3", "file:"+arg.URI+"?mode=rwc&_journal_mode=WAL&_synchronous=0&_locking_mode=EXCLUSIVE"); err != nil {
		return nil, err
	}

	if _, err = db.Exec("BEGIN"); err != nil {
		return nil, err
	}

	return &sqliteSink{&arg, pipe, db}, nil
}

//////////////////////// Sink

type sqliteSink struct {
	args *sqliteSinkArgs
	pipe *swllib.Pipe
	db   *sql.DB
}

func (s *sqliteSink) OnError(_ error) {
	// send rollback
	_, _ = s.db.Exec("ROLLBACK")
	_ = s.db.Close()
}

func (s *sqliteSink) OnEnd() error {
	if _, err := s.db.Exec("COMMIT"); err != nil {
		return err
	}
	return s.db.Close()
}

func (s *sqliteSink) OnCollectionStart(start *swllib.CollectionStartChunk) (swllib.CollectionHandler, error) {
	var (
		stmt    *sql.Stmt
		err     error
		columns = make([]string, len(start.TypeHints))
		types   = make([]string, len(start.TypeHints))
	)

	i := 0
	for k, v := range start.TypeHints {
		columns[i] = k
		types[i] = v // SHOULD PROBABLY REORDER THAT
		i++
	}

	// Table may be dropped and then recreated.
	if s.args.Drop {
		if _, err = s.db.Exec(fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, start.Name)); err != nil {
			return nil, err
		}
	}

	// Recreate the table
	// Need more type hints !
	// how do I do this ?

	// If we need to truncate, to it now.
	if s.args.Truncate {
		if _, err = s.db.Exec(fmt.Sprintf(`DELETE FROM TABLE "%s"`, start.Name)); err != nil {
			return nil, err
		}
	}

	if stmt, err = s.db.Prepare(`INSERT INTO `); err != nil {
		// Wrap !
		return nil, err
	}

	return &sqliteColHandler{columns, s.pipe, stmt}, nil
}

//////////////////////// Collection handler

type sqliteColHandler struct {
	columns []string
	pipe    *swllib.Pipe
	stmt    *sql.Stmt
}

func (ch *sqliteColHandler) OnData(data swllib.Data, _ uint) error {
	var args = make([]interface{}, len(ch.columns))
	for i, c := range ch.columns {
		if val, ok := data[c]; ok {
			args[i] = val
		}
	}
	// transform data into []interface{}.
	// this should be done by knowing columns...
	_, err := ch.stmt.Exec(args... /* ... */)
	return err
}

func (ch *sqliteColHandler) OnEnd() error {
	// Close the statement.
	return ch.stmt.Close()
}
