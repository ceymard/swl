package tester

import (
	"github.com/ceymard/swl/swllib"
)

func init() {
	swllib.RegisterSource("tester", "tester source", TesterSourceCreator)
}

func TesterSourceCreator(pipe *swllib.Pipe, args []string) (swllib.Source, error) {
	return &Tester{pipe}, nil
}

type Tester struct {
	pipe *swllib.Pipe
}

func (t *Tester) Emit() error {
	var err error

	err = t.pipe.WriteStartCollection(&swllib.CollectionStartChunk{Name: "test-1"})

	for i := 0; err == nil && i < 12; i++ {
		err = t.pipe.WriteData(swllib.Data{"Hello": 1, "World": i, "Test": "str"})
	}

	err = t.pipe.WriteData(swllib.Data{"testing": []string{"hey", "ho"}})

	t.pipe.Close()

	if err != nil {
		// log.Print("%v", err)
	}
	return nil
}
