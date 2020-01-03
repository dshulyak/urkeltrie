package store

import (
	"bufio"
)

func newBuffered(f *file, bufSize int) *buffered {
	return &buffered{file: f, bufSize: bufSize}
}

type buffered struct {
	file    *file
	buf     *bufio.Writer
	bufSize int
}

func (b *buffered) Write(buf []byte) (int, error) {
	if b.buf == nil {
		b.buf = bufio.NewWriterSize(b.file, b.bufSize)
	}
	return b.buf.Write(buf)
}

func (b *buffered) Flush() error {
	return b.buf.Flush()
}

func (b *buffered) Commit() error {
	if err := b.buf.Flush(); err != nil {
		return err
	}
	return b.file.Commit()
}

func (b *buffered) Reset() {
	b.buf = nil
}
