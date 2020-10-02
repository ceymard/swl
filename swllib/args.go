package swllib

import "github.com/alecthomas/kong"

// ParseArgs parses args for a cli
func ParseArgs(dst interface{}, args []string, options ...kong.Option) error {
	var (
		err    error
		parser *kong.Kong
	)

	parser, err = kong.New(dst, options...)

	if err == nil {
		_, err = parser.Parse(args)
	}

	return err
}
