package main

import (
	"sync"

	"github.com/ceymard/swl/debug"
	"github.com/ceymard/swl/swllib"
	"github.com/ceymard/swl/tester"
)

func main() {
	// Sources
	swllib.RegisterSource("test", "sends test data", tester.TesterSourceCreator)

	// Sinks
	swllib.RegisterSink("debug", "prints chunks to the console", debug.DebugSinkCreator)

	// We're gonna split the args into different commands
	var (
		// args       = os.Args[1:]
		nbcommands int
		wg         sync.WaitGroup
		commands   []CommandBlock
	)

	//// FOR TEST PURPOSES
	// pp.Print(args)
	commands = []CommandBlock{
		{source: true, args: []string{}, srcCreator: tester.TesterSourceCreator},
		{source: false, args: []string{}, sinkCreator: debug.DebugSinkCreator},
	}

	/////////////////////
	nbcommands = len(commands)
	wg.Add(nbcommands)

	channels := make([]*swllib.Channel, nbcommands)
	for i := 0; i < nbcommands; i++ {
		channels[i] = swllib.NewChannel(8192)
	}

	pipes := make([]*swllib.Pipe, nbcommands)
	for i := 0; i < nbcommands; i++ {
		var prev_pipe *swllib.Pipe = nil
		if i > 0 {
			prev_pipe = pipes[i-1]
		}
		pipes[i] = swllib.NewPipe(prev_pipe, channels[i])
	}

	for i := 0; i < nbcommands; i++ {
		var cmd = commands[i]
		if cmd.source {
			swllib.RunSource(&wg, pipes[i], cmd.args, cmd.srcCreator)
		} else {
			swllib.RunSink(&wg, pipes[i], cmd.args, cmd.sinkCreator)
		}
	}

	wg.Wait()
}

type CommandBlock struct {
	source      bool
	args        []string
	srcCreator  swllib.SourceCreator
	sinkCreator swllib.SinkCreator
}
