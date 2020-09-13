package main

import (
	"errors"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/ceymard/swl/swllib"

	_ "github.com/ceymard/swl/debug"
	_ "github.com/ceymard/swl/sqlite"
	_ "github.com/ceymard/swl/tester"
)

func main() {

	// We're gonna split the args into different commands
	var (
		// args       = os.Args[1:]
		err        error
		nbcommands int
		wg         sync.WaitGroup
		commands   []CommandBlock
	)

	//// FOR TEST PURPOSES
	if commands, err = parseArgs(); err != nil {
		log.Print(err)
		os.Exit(1)
	}
	// pp.Print(commands)
	// pp.Print(args)
	// commands = []CommandBlock{
	// 	{source: true, args: []string{}, srcCreator: tester.TesterSourceCreator},
	// 	{source: false, args: []string{}, sinkCreator: debug.DebugSinkCreator},
	// }

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
		if cmd.IsSource {
			if err := swllib.RunSource(&wg, pipes[i], cmd.Name, cmd.Args, cmd.SrcCreator); err != nil {
				log.Print(err)
				os.Exit(1)
			}
		} else {
			if err = swllib.RunSink(&wg, pipes[i], cmd.Name, cmd.Args, cmd.SinkCreator); err != nil {
				log.Print(err)
				os.Exit(1)
			}
		}
	}

	wg.Wait()
}

type CommandBlock struct {
	IsSource    bool
	Name        string
	Args        []string
	SrcCreator  swllib.SourceCreator
	SinkCreator swllib.SinkCreator
}

func parseArgs() ([]CommandBlock, error) {
	var (
		isSource       = true
		args           = append(os.Args[1:], "::", "debug")
		name           = ""
		lookingForName = true
		acc            = make([]string, 0, 16)
		res            = make([]CommandBlock, 0, 24)
	)

	var addToCommands = func() error {

		var cmd = CommandBlock{
			IsSource: isSource,
			Args:     acc,
			Name:     name,
		}

		if srcc, ok := swllib.Sources[name]; ok && isSource {
			cmd.SrcCreator = srcc.Creator
		} else if wrcc, ok := swllib.Sinks[name]; ok && !isSource {
			cmd.SinkCreator = wrcc.Creator
		} else {
			return fmt.Errorf(`Could not find handler for '%s'`, name)
		}
		res = append(res, cmd)
		acc = make([]string, 0, 24)
		return nil
	}

	for _, arg := range args {
		if lookingForName {
			lookingForName = false
			name = arg
		} else if arg == "::" || arg == "++" {
			if lookingForName {
				break
			}
			lookingForName = true
			if err := addToCommands(); err != nil {
				return nil, err
			}

			isSource = arg == "++"
		} else {
			acc = append(acc, arg)
		}
	}

	if lookingForName {
		return nil, errors.New(`missing a source or sink`)
	}
	if err := addToCommands(); err != nil {
		return nil, err
	}

	return res, nil
}
