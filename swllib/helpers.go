package swllib

import "bytes"

// Buffer is a wrapper around bytes.Buffer
type Buffer struct {
	bytes.Buffer
}

// WriteStrings Writes several strings in one go
func (b *Buffer) WriteStrings(strs ...string) error {
	for _, s := range strs {
		if _, err := b.WriteString(s); err != nil {
			return err
		}
	}
	return nil
}
