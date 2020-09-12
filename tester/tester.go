package tester

import (
	"github.com/ceymard/swl/swllib"
)

func TesterSourceCreator(pipe *swllib.Pipe, args []string) (swllib.Source, error) {
	return &Tester{pipe}, nil
}

type Tester struct {
	pipe *swllib.Pipe
}

func (t *Tester) Emit() error {
	var err error

	err = t.pipe.WriteStartCollection("test-1")

	for i := 0; err == nil && i < 12; i++ {
		err = t.pipe.WriteData(swllib.Data{"Hello": 1, "World": i, "Test": "str"})
	}

	t.pipe.Close()

	if err != nil {
		// log.Print("%v", err)
	}
	return nil
}
