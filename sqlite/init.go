package swlite

import "github.com/ceymard/swl/swllib"

func init() {
	swllib.RegisterSource(SqliteSourceCreator, "sqlite handler", "sqlite",
		".sqlite",
		".sqlite3",
		".db",
		"sqlite://",
	)
}
