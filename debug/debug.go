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

func (d *DebugSink) OnCollectionStart(start *swllib.CollectionStartChunk, data []swllib.Data) (swllib.CollectionHandler, error) {
	d.col = start.Name
	// pp.Print(start.TypeHints)
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
			fmt.Print("[")
			for i, l := 0, ref.Len(); i < l; i++ {
				if i > 0 {
					fmt.Print(", ")
				}
				pretty(ref.Index(i).Interface())
			}
			fmt.Print("]")
			// fmt.Printf("slice: len=%d, %v\n", ref.Len(), ref.Interface())
		case reflect.Map:
			fmt.Print("{")
			for i, key := range ref.MapKeys() {
				if i > 0 {
					fmt.Print(", ")
				}
				pretty(key.Interface())
				fmt.Print(": ")
				var elt = ref.MapIndex(key)
				pretty(elt.Interface())
			}
			fmt.Print("}")
			// fmt.Printf("map: %v\n", ref.Interface())
		case reflect.Chan:
			fmt.Printf("chan %v\n", ref.Interface())
		default:
			fmt.Printf(`%v`, ref)
		}

	}
}
