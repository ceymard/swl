package debug

import (
	"errors"

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

	return &DebugSink{}, nil
}

func (d *DebugSink) OnError(err error) {
	pp.Print(err)
}

func (d *DebugSink) OnCollectionStart(name string) (swllib.CollectionHandler, error) {
	_, err := pp.Printf("Collection start %v\n", name)
	d.col = name
	return d, err
}

func (d *DebugSink) OnCollectionEnd() error {
	return nil
}

func (d *DebugSink) OnData(data map[string]interface{}, idx uint) error {
	_, err := pp.Printf("%s:%v: %v\n", d.col, int(idx), data)
	return err
}

func (d *DebugSink) OnEnd() error {
	return nil
}
