package swlite

import "github.com/ceymard/swl/swllib"

func init() {
	swllib.RegisterSource("sqlite", "sqlite handler", SqliteSourceCreator)
}
