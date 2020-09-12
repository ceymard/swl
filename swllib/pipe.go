package swllib

import (
	"errors"
)

const (
	ALL_GOOD = iota
	WAITING_FOR_WRITER
	WAITING_FOR_READER
)

// Channel connects two or more handlers.
// The writing handler needs to wait if the buffer is full and reader did not consume it.
// The reading handler needs to wait for the writer to fill its buffer
type Channel struct {
	mailbox *Mailbox
}

func NewChannel(bufsize int) *Channel {
	p := &Channel{mailbox: NewMailbox(bufsize)}
	return p
}

/////////////////////////////// Functions called by the writer

func (p *Channel) writeChunk(chk interface{}) error {
	if p.mailbox.isClosed() {
		// log.Print("trying to write on a closed pipe")
		// debug.PrintStack()
		return errors.New(`pipe is closed`)
	}

	p.mailbox.Send(chk, true)

	switch chk.(type) {
	case error:
		p.mailbox.Close()
	}
	return nil
}

////////////////////////////// Functions called by the handler

// Get all the chunks in the buffer
// Returns a slice of chunks and a boolean that indicates whether
// the pipe is closed.
func (p *Channel) Read() (interface{}, bool) {
	chk, state := p.mailbox.Receive(true)
	return chk, state == StateClosed
}

type Pipe struct {
	upstream *Pipe
	write    *Channel
}

func NewPipe(upstream *Pipe, write *Channel) *Pipe {
	return &Pipe{upstream, write}
}

func (p *Pipe) WriteStartCollection(name string) error {
	return p.write.writeChunk(&CollectionStartChunk{Name: name})
}

func (p *Pipe) WriteData(payload map[string]interface{}) error {
	return p.write.writeChunk(payload)
}

func (p *Pipe) WriteError(err error) error {
	return p.write.writeChunk(err)
}

func (p *Pipe) Close() {
	if p.upstream != nil && !p.upstream.write.mailbox.isClosed() {
		p.upstream.Close()
	}

	p.write.mailbox.Close()
}
