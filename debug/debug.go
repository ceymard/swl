package debug

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/ceymard/swl/swllib"
	"github.com/fatih/color"
	"github.com/k0kubun/pp"
)

func init() {
	swllib.RegisterSink(debugSinkCreator, "prints chunks to the console", "debug")
}

type debugSink struct {
	col string
}

func debugSinkCreator(_ *swllib.Pipe, args []string) (swllib.Sink, error) {
	if len(args) > 0 {
		return nil, errors.New("debug does not accept arguments")
	}

	// Should probably read args to disable colors
	color.NoColor = false
	return &debugSink{}, nil
}

// OnError .
func (d *debugSink) OnError(err error) {
	pp.Print(err)
}

// OnCollectionStart .
func (d *debugSink) OnCollectionStart(start *swllib.CollectionStartChunk) (swllib.CollectionHandler, error) {
	d.col = start.Name
	cy.Print(d.col, ` typehints `)
	pretty(start.TypeHints)
	_, _ = fmt.Print("\n")

	// pp.Print(start.TypeHints)
	return d, nil
}

// OnCollectionEnd .
func (d *debugSink) OnCollectionEnd() error {
	return nil
}

var (
	cy       = color.New(color.Bold, color.FgCyan)
	ge       = color.New(color.Bold, color.FgHiGreen)
	coProp   = color.New(color.FgGreen, color.Faint)
	coString = color.New(color.FgGreen)
	coNull   = color.New(color.FgRed)
	coNum    = color.New(color.FgHiMagenta)
)

// OnData .
func (d *debugSink) OnData(data swllib.Data, idx uint) error {
	cy.Print(d.col, ` `)
	ge.Print(int(idx))
	_, _ = fmt.Print(` `)
	pretty(data)
	_, _ = fmt.Print("\n")
	return nil
}

// OnEnd .
func (d *debugSink) OnEnd() error {
	return nil
}

func pretty(v interface{}) {
	switch v.(type) {
	case nil:
		coNull.Print("null")
	case bool:
		coNull.Print(v)
	case float32, float64, int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		coNum.Print(v)
	case string:
		var s = v.(string)
		if strings.Contains(s, " ") || len(s) == 0 {
			coString.Print(`"`, s, `"`)
		} else {
			coString.Print(v)
		}
	case swllib.Data:

		var (
			keys = make([]string, 0, 24)
			mp   = v.(swllib.Data)
		)

		for k := range mp {
			keys = append(keys, k)
		}

		sort.Strings(keys)

		for i, k := range keys {
			if i > 0 {
				_, _ = fmt.Print(`, `)
			}
			coProp.Print(color.GreenString(k), `: `)
			pretty(mp[k])
		}

	default:
		ref := reflect.ValueOf(v)
		switch ref.Kind() {
		// case reflect.Bool:
		// 	fmt.Printf("bool: %v\n", ref.Bool())
		// case reflect.Int, reflect.Int8, reflect.Int32, reflect.Int64:
		// 	fmt.Printf("int: %v\n", ref.Int())
		// case reflect.Uint, reflect.Uint8, reflect.Uint32, reflect.Uint64:
		// 	fmt.Printf("int: %v\n", ref.Uint())
		// case reflect.Float32, reflect.Float64:
		// 	fmt.Printf("float: %v\n", ref.Float())
		// case reflect.String:
		// 	fmt.Printf("string: %v\n", ref.String())
		case reflect.Slice:
			_, _ = fmt.Print("[")
			for i, l := 0, ref.Len(); i < l; i++ {
				if i > 0 {
					_, _ = fmt.Print(", ")
				}
				pretty(ref.Index(i).Interface())
			}
			_, _ = fmt.Print("]")
			// fmt.Printf("slice: len=%d, %v\n", ref.Len(), ref.Interface())
		case reflect.Map:
			_, _ = fmt.Print("{")
			for i, key := range ref.MapKeys() {
				if i > 0 {
					_, _ = fmt.Print(", ")
				}
				pretty(key.Interface())
				_, _ = fmt.Print(": ")
				var elt = ref.MapIndex(key)
				pretty(elt.Interface())
			}
			_, _ = fmt.Print("}")
			// fmt.Printf("map: %v\n", ref.Interface())
		case reflect.Chan:
			_, _ = fmt.Printf("chan %v\n", ref.Interface())
		default:
			_, _ = fmt.Printf(`%v`, ref)
		}

	}
}
