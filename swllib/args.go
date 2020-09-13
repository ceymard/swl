package swllib

import "github.com/alecthomas/kong"

func ParseArgs(dst interface{}, args []string) error {
	var (
		err    error
		parser *kong.Kong
	)

	parser, err = kong.New(dst)

	if err == nil {
		_, err = parser.Parse(args)
	}

	return err
}
