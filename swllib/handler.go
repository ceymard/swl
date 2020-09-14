package swllib

import (
	"fmt"
	"path"
	"regexp"
	"sync"
)

type CollectionHandler interface {
	OnData(ch Data, index uint) error
	OnEnd() error
}

type Sink interface {
	// Parse the arguments given by the command line
	OnCollectionStart(start *CollectionStartChunk, firstData []Data) (CollectionHandler, error)
	OnEnd() error
	OnError(err error)
}

type Source interface {
	Emit() error
}

type SourceCreator func(pipe *Pipe, args []string) (Source, error)
type SinkCreator func(pipe *Pipe, args []string) (Sink, error)

func RunSource(wg *sync.WaitGroup, pipe *Pipe, name string, args []string, srcc SourceCreator) error {
	src, err := srcc(pipe, args)
	if err != nil {
		return fmt.Errorf("in handler '%s': %w", name, err)
	}

	go func() {
		defer func() {
			pipe.Close()
			wg.Done()
		}()

		var (
			err    error
			chk    interface{}
			closed bool
			up     *Channel
			w      = pipe.write
		)

		if pipe.upstream != nil {
			up = pipe.upstream.write
		}

		if up != nil {
			for chk, closed = up.Read(); err == nil && !closed; chk, closed = up.Read() {
				if errr, ok := chk.(error); ok {
					err = errr
					break
				} else {
					w.writeChunk(chk)
				}
			}
		}

		if err != nil {
			pipe.WriteError(err)
			return
		}

		err = src.Emit()

		if err != nil {
			err = fmt.Errorf("in handler '%s': %w", name, err)
			pipe.WriteError(err)
		}
	}()

	return nil
}

func RunSink(wg *sync.WaitGroup, pipe *Pipe, name string, args []string, sinkc SinkCreator) error {
	sink, err := sinkc(pipe, args)
	if err != nil {
		return fmt.Errorf("in handler '%s': %w", name, err)
	}

	// Start our handler
	go (func() {
		defer func() {
			pipe.Close()
			wg.Done()
		}()

		var (
			firstCol = true
			colhld   CollectionHandler
			err      error
			closed   bool
			chk      interface{}
			index    uint = 1
			up            = pipe.upstream.write
		)

		// missing command handling.
		for chk, closed = up.Read(); err == nil && !closed; chk, closed = up.Read() {
			if dt, ok := chk.(map[string]interface{}); ok {
				err = colhld.OnData(dt, index)
				index++
			} else if start, ok := chk.(*CollectionStartChunk); ok {
				if !firstCol {
					colhld.OnEnd()
					firstCol = false
				}
				index = 1
				// now entering buffered mode for a few data buffers

				colhld, err = sink.OnCollectionStart(start, []Data{})
			} else if errr, ok := chk.(error); ok {
				pipe.WriteError(errr)
				sink.OnError(errr)
				return
			}

			if err != nil {
				err = fmt.Errorf("in handler '%s': %w", name, err)
				pipe.WriteError(err)
				sink.OnError(err)
				return
			}
		}

	})()

	return nil
}

////////////////////////////////////////////////////////////////

type RegisteredSource struct {
	Name    string
	Help    string
	Creator SourceCreator
}

type RegisteredSink struct {
	Name    string
	Help    string
	Creator SinkCreator
}

var Sources = make(map[string]*RegisteredSource)
var Sinks = make(map[string]*RegisteredSink)

func RegisterSource(cbk SourceCreator, help string, names ...string) {
	for _, name := range names {
		Sources[name] = &RegisteredSource{name, help, cbk}
	}
}

func RegisterSink(cbk SinkCreator, help string, names ...string) {
	for _, name := range names {
		Sinks[name] = &RegisteredSink{name, help, cbk}
	}
}

var reProto = regexp.MustCompile(`^[-a-zA-Z_]+://`)

func GetSource(pth string) (*RegisteredSource, bool) {

	if reg, ok := Sources[pth]; ok {
		return reg, false
	}

	// look for proto://
	proto := reProto.FindString(pth)
	if reg, ok := Sources[proto]; ok {
		return reg, true
	}

	// lastly, look for extension
	ext := path.Ext(pth)
	if reg, ok := Sources[ext]; ok {
		return reg, true
	}

	return nil, false
}

func GetSink(pth string) (*RegisteredSink, bool) {

	if reg, ok := Sinks[pth]; ok {
		return reg, false
	}

	// look for proto://
	proto := reProto.FindString(pth)
	if reg, ok := Sinks[proto]; ok {
		return reg, true
	}

	// lastly, look for extension
	ext := "." + path.Ext(pth)
	if reg, ok := Sinks[ext]; ok {
		return reg, true
	}

	return nil, false
}
