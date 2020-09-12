package debug

import (
	"errors"
	"fmt"
	"sort"

	"github.com/ceymard/swl/swllib"
	"github.com/fatih/color"
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
	color.NoColor = false
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

var (
	CY       = color.New(color.Bold, color.FgCyan)
	GE       = color.New(color.Bold, color.FgHiGreen)
	coProp   = color.New(color.FgGreen, color.Faint)
	coString = color.New(color.FgGreen)
	coNull   = color.New(color.FgRed)
	coNum    = color.New(color.FgHiMagenta)
)

func (d *DebugSink) OnData(data swllib.Data, idx uint) error {
	CY.Print(d.col, ` `)
	GE.Print(int(idx))
	fmt.Print(` `)
	pretty(data)
	fmt.Print("\n")
	return nil
}

func (d *DebugSink) OnEnd() error {
	return nil
}

func pretty(v interface{}) {
	switch v.(type) {
	case []interface{}:

	case int:
		coNum.Print(v)
	case string:
		coString.Print(`"`, v, `"`)
	case swllib.Data:

		var (
			keys = make([]string, 0, 24)
			mp   = v.(swllib.Data)
		)

		for k, _ := range mp {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		for i, k := range keys {
			if i > 0 {
				fmt.Print(`, `)
			}
			coProp.Print(color.GreenString(k), `: `)
			pretty(mp[k])
		}

	default:
		fmt.Printf(`%v`, v)
	}
}
