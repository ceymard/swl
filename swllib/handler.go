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
	OnCollectionStart(start *CollectionStartChunk) (CollectionHandler, error)
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
				if err = colhld.OnData(dt, index); err != nil {
					break
				}
				index++
			} else if start, ok := chk.(*CollectionStartChunk); ok {
				if !firstCol {
					if err = colhld.OnEnd(); err != nil {
						break
					}
					firstCol = false

				}
				index = 1
				// now entering buffered mode for a few data buffers

				if colhld, err = sink.OnCollectionStart(start); err != nil {
					break
				}
			} else if errr, ok := chk.(error); ok {
				_ = pipe.WriteError(errr)
				sink.OnError(errr)
				return
			}
		}

		if !firstCol {
			err = colhld.OnEnd()
		}

		if err == nil {
			err = sink.OnEnd()
		}

		if err != nil {
			err = fmt.Errorf("in handler '%s': %w", name, err)
			_ = pipe.WriteError(err)
			sink.OnError(err)
			return
		}

	})()

	return nil
}

////////////////////////////////////////////////////////////////

// RegisteredSource A registered source
type RegisteredSource struct {
	Name    string
	Help    string
	Creator SourceCreator
}

// RegisteredSink is A registered sink
type RegisteredSink struct {
	Name    string
	Help    string
	Creator SinkCreator
}

var sources = make(map[string]*RegisteredSource)
var sinks = make(map[string]*RegisteredSink)

// RegisterSource registers a source
func RegisterSource(cbk SourceCreator, help string, names ...string) {
	for _, name := range names {
		sources[name] = &RegisteredSource{name, help, cbk}
	}
}

// RegisterSink registers a sink
func RegisterSink(cbk SinkCreator, help string, names ...string) {
	for _, name := range names {
		sinks[name] = &RegisteredSink{name, help, cbk}
	}
}

var reProto = regexp.MustCompile(`^[-a-zA-Z_]+://`)

// GetSource gets asource
func GetSource(pth string) (*RegisteredSource, bool) {

	if reg, ok := sources[pth]; ok {
		return reg, false
	}

	// look for proto://
	proto := reProto.FindString(pth)
	if reg, ok := sources[proto]; ok {
		return reg, true
	}

	// lastly, look for extension
	ext := path.Ext(pth)
	if reg, ok := sources[ext]; ok {
		return reg, true
	}

	return nil, false
}

// GetSink Gets a Sink
func GetSink(pth string) (*RegisteredSink, bool) {

	if reg, ok := sinks[pth]; ok {
		return reg, false
	}

	// look for proto://
	proto := reProto.FindString(pth)
	if reg, ok := sinks[proto]; ok {
		return reg, true
	}

	// lastly, look for extension
	ext := path.Ext(pth)
	if reg, ok := sinks[ext]; ok {
		return reg, true
	}

	return nil, false
}
