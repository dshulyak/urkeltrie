package store

import (
	"bufio"
)

func newBuffered(f *file, bufSize int) *buffered {
	return &buffered{file: f, bufSize: bufSize}
}

type buffered struct {
	file       *file
	buf        *bufio.Writer
	bufSize    int
	flushSize  uint64
	flushCount uint64
}

func (b *buffered) Write(buf []byte) (int, error) {
	if b.buf == nil {
		b.buf = bufio.NewWriterSize(b.file, b.bufSize)
	}
	before := b.buf.Buffered()
	n, err := b.buf.Write(buf)
	if b.buf.Buffered() < before {
		b.flushSize += uint64(b.bufSize)
		b.flushCount++
	}
	return n, err
}

func (b *buffered) Flush() error {
	buffered := b.buf.Buffered()
	if buffered != 0 {
		b.flushSize += uint64(buffered)
		b.flushCount++
	}
	return b.buf.Flush()
}

func (b *buffered) Commit() error {
	if err := b.Flush(); err != nil {
		return err
	}
	return b.file.Commit()
}

func (b *buffered) Reset() {
	b.buf = nil
}

func (b *buffered) ReadStats(stats *GroupStats) {
	stats.FlushSize += b.flushSize
	stats.FlushCount += b.flushCount
}
