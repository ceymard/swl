package swllib

import (
	"sync"
)

type CollectionHandler interface {
	OnData(ch Data, index uint) error
	OnEnd() error
}

type Sink interface {
	// Parse the arguments given by the command line
	OnCollectionStart(name string) (CollectionHandler, error)
	OnEnd() error
	OnError(err error)
}

type Source interface {
	Emit() error
}

type SourceCreator func(pipe *Pipe, args []string) (Source, error)
type SinkCreator func(pipe *Pipe, args []string) (Sink, error)

func RunSource(wg *sync.WaitGroup, pipe *Pipe, args []string, srcc SourceCreator) {
	go func() {
		defer func() {
			pipe.Close()
			wg.Done()
		}()

		var (
			src    Source
			err    error
			chk    interface{}
			closed bool
			up     *Channel
			w      = pipe.write
		)

		src, err = srcc(pipe, args)

		if pipe.upstream != nil {
			up = pipe.upstream.write
		}

		if up != nil {
			for chk, closed = up.Read(); err == nil && !closed; chk, closed = up.Read() {
				if errr, ok := chk.(error); ok {
					err = errr
				} else {
					w.writeChunk(chk)
				}
			}
		}

		if err == nil {
			err = src.Emit()
		}

		if err != nil {
			pipe.WriteError(err)
		}
	}()
}

func RunSink(wg *sync.WaitGroup, pipe *Pipe, args []string, sinkc SinkCreator) {
	// Start our handler
	go (func() {
		defer func() {
			pipe.Close()
			wg.Done()
		}()

		var (
			sink     Sink
			firstCol = true
			colhld   CollectionHandler
			err      error
			closed   bool
			chk      interface{}
			index    uint = 1
			up            = pipe.upstream.write
		)

		sink, err = sinkc(pipe, args)

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
				colhld, err = sink.OnCollectionStart(start.Name)
			} else if errr, ok := chk.(error); ok {
				err = errr
			}
		}

		if err != nil {
			pipe.WriteError(err)
			sink.OnError(err)
		}

	})()
}

////////////////////////////////////////////////////////////////

type RegisteredSource struct {
	Name    string
	Help    string
	Creator SourceCreator
}

var Sources = make(map[string]RegisteredSource)

type RegisteredSink struct {
	Name    string
	Help    string
	Creator SinkCreator
}

var Sinks = make(map[string]RegisteredSink)

func RegisterSource(name string, help string, cbk SourceCreator) {
	Sources[name] = RegisteredSource{name, help, cbk}
}

func RegisterSink(name string, help string, cbk SinkCreator) {
	Sinks[name] = RegisteredSink{name, help, cbk}
}
