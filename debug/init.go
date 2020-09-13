package debug

import "github.com/ceymard/swl/swllib"

func init() {
	swllib.RegisterSink("debug", "prints chunks to the console", DebugSinkCreator)
}
