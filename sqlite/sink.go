package swlite

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

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

func (s *sqliteSink) exec(query string) error {
	if _, err := s.db.Exec(query); err != nil {
		return fmt.Errorf(`in statement '%s' %w`, query, err)
	}
	return nil
}

func (s *sqliteSink) prepare(query string) (*sql.Stmt, error) {
	var (
		stmt *sql.Stmt
		err  error
	)
	if stmt, err = s.db.Prepare(query); err != nil {
		return nil, fmt.Errorf(`error when preparing '%s' %w`, query, err)
	}
	return stmt, nil
}

func (s *sqliteSink) OnError(_ error) {
	// send rollback
	_, _ = s.db.Exec("ROLLBACK")
	_ = s.db.Close()
}

func (s *sqliteSink) OnEnd() error {
	defer s.db.Close()
	if _, err := s.db.Exec("COMMIT"); err != nil {
		return err
	}
	return nil
}

func (s *sqliteSink) OnCollectionStart(start *swllib.CollectionStartChunk) (swllib.CollectionHandler, error) {
	var (
		stmt    *sql.Stmt
		err     error
		columns = make([]string, len(start.TypeHints))
		query   swllib.Buffer
	)

	// Table may be dropped and then recreated.
	if s.args.Drop {
		if err = s.exec(fmt.Sprintf(`DROP TABLE IF EXISTS "%s"`, start.Name)); err != nil {
			return nil, err
		}
	}

	// Create table
	_ = query.WriteStrings(`CREATE TABLE IF NOT EXISTS "`, start.Name, `" (`)

	// Get the hints
	i := 0
	for k, v := range start.TypeHints {
		columns[i] = k
		if i > 0 {
			_ = query.WriteStrings(", ")
		}
		_ = query.WriteStrings(k, " ", v)
		i++
	}

	_, _ = query.WriteString(`)`)

	// log.Print(query.String())
	if err = s.exec(query.String()); err != nil {
		return nil, err
	}

	// Recreate the table
	// Need more type hints !
	// how do I do this ?

	// If we need to truncate, to it now.
	if s.args.Truncate {
		if err = s.exec(fmt.Sprintf(`DELETE FROM "%s"`, start.Name)); err != nil {
			return nil, err
		}
	}

	query = swllib.Buffer{}
	query.WriteStrings(`INSERT `)
	if s.args.Upsert {
		query.WriteStrings(`OR REPLACE `)
	}
	query.WriteStrings(`INTO "`, start.Name, `" (`, strings.Join(columns, ", "), ") VALUES (")
	for i := range columns {
		if i > 0 {
			query.WriteString(", ?")
		} else {
			query.WriteString("?")
		}
	}
	query.WriteStrings(`)`)

	if stmt, err = s.prepare(query.String()); err != nil {
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
	var (
		args  = make([]interface{}, len(ch.columns))
		bytes []byte
		err   error
	)
	for i, c := range ch.columns {
		if val, ok := data[c]; ok {
			if !isNilFixed(val) {
				var rt = reflect.TypeOf(val)
				var kind = rt.Kind()
				if kind == reflect.Slice || kind == reflect.Map {

					if bytes, err = json.Marshal(val); err != nil {
						return fmt.Errorf(`unable to convert %v to json %w`, val, err)
					}
					args[i] = string(bytes)
					continue
				}
			}

			args[i] = val
		}
	}
	// transform data into []interface{}.
	// this should be done by knowing columns...
	_, err = ch.stmt.Exec(args... /* ... */)
	return err
}

func (ch *sqliteColHandler) OnEnd() error {
	// Close the statement.
	return ch.stmt.Close()
}

func isNilFixed(i interface{}) bool {
	if i == nil {
		return true
	}
	switch reflect.TypeOf(i).Kind() {
	case reflect.Ptr, reflect.Map, reflect.Array, reflect.Chan, reflect.Slice:
		return reflect.ValueOf(i).IsNil()
	}
	return false
}
