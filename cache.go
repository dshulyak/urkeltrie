package urkeltrie

import (
	"io"
)

const (
	rbufsize  = 2048
	maxChunks = 5
)

func newChunk(buf []byte, off, lth int64) *chunk {
	return &chunk{off: off, limit: off + lth, buf: buf}
}

type chunk struct {
	off, limit int64
	buf        []byte
}

func (c *chunk) in(off, size int64) bool {
	return off >= c.off && off+size <= c.limit
}

func (c *chunk) ReadAt(buf []byte, off int64) (int, error) {
	diff := off - c.off
	return copy(buf, c.buf[diff:]), nil
}

func newOffsetCache() *offsetCache {
	return &offsetCache{chunks: make([]*chunk, 0, maxChunks)}
}

type offsetCache struct {
	hit, miss uint64
	chunks    []*chunk
}

func (oc *offsetCache) ReadAt(buf []byte, off int64) (int, error) {
	size := int64(len(buf))
	for _, c := range oc.chunks {
		if c.in(off, size) {
			oc.hit++
			return c.ReadAt(buf, off)
		}
	}
	oc.miss++
	return 0, nil
}

func (oc *offsetCache) Update(buf []byte, off, limit int64) {
	c := newChunk(buf, off, limit)
	if len(oc.chunks) < maxChunks {
		oc.chunks = append(oc.chunks, c)
	} else {
		copy(oc.chunks[1:], oc.chunks)
		oc.chunks[0] = c
	}
}

func (oc *offsetCache) GetBuf() []byte {
	if len(oc.chunks) < maxChunks {
		return make([]byte, rbufsize)
	}
	return oc.chunks[len(oc.chunks)-1].buf
}

func NewCachingFile(f *File) *CachingFile {
	return &CachingFile{File: f, cache: newOffsetCache()}
}

type CachingFile struct {
	*File
	cache *offsetCache
}

func (cf *CachingFile) ReadAt(buf []byte, off int64) (int, error) {
	n, err := cf.cache.ReadAt(buf, off)
	if n > 0 {
		return n, err
	}
	// TODO one from cache can be used
	rbuf := cf.cache.GetBuf()
	n, err = cf.File.ReadAt(rbuf, off)
	if err != nil && err != io.EOF {
		return 0, err
	}
	cf.cache.Update(rbuf, off, int64(n))
	return copy(buf, rbuf), nil
}
