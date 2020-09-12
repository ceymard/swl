package debug

import (
	"errors"
	"strings"

	"github.com/ceymard/swl/swllib"
	"github.com/k0kubun/pp"
)

type DebugSink struct {
	col string
}

func DebugSinkCreator(pipe *swllib.Pipe, args []string) (swllib.Sink, error) {
	if len(args) > 0 {
		return nil, errors.New("debug does not accept arguments")
	}

	// Should probably read args to disable colors
	pp.PrintMapTypes = false
	return &DebugSink{}, nil
}

func (d *DebugSink) OnError(err error) {
	pp.Print(err)
}

func (d *DebugSink) OnCollectionStart(name string) (swllib.CollectionHandler, error) {
	d.col = name
	return d, nil
}

func (d *DebugSink) OnCollectionEnd() error {
	return nil
}

func (d *DebugSink) OnData(data swllib.Data, idx uint) error {
	var s = pp.Sprintf("%s@%v: %v", d.col, int(idx), data)
	print(strings.ReplaceAll(s, "\n", ""), "\n")
	return nil
}

func (d *DebugSink) OnEnd() error {
	return nil
}

// func
