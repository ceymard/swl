package debug

import "github.com/ceymard/swl/swllib"

func init() {
	swllib.RegisterSink(DebugSinkCreator, "prints chunks to the console", "debug")
}
