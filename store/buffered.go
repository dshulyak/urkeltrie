package store

import (
	"bufio"
)

func newWritersPool(bufSize int) *writersPool {
	return &writersPool{bufSize: bufSize}
}

type writersPool struct {
	bufSize int
	bufs    []*bufio.Writer
}

func (pool *writersPool) Get() *bufio.Writer {
	if len(pool.bufs) > 0 {
		buf := pool.bufs[0]
		pool.bufs = pool.bufs[1:]
		return buf
	}
	return bufio.NewWriterSize(nil, pool.bufSize)
}

func (pool *writersPool) Put(buf *bufio.Writer) {
	pool.bufs = append(pool.bufs, buf)
}

func newBuffered(f *file, pool *writersPool, bufSize int) *buffered {
	return &buffered{file: f, pool: pool, bufSize: bufSize}
}

type buffered struct {
	file    *file
	buf     *bufio.Writer
	bufSize int
	pool    *writersPool
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
