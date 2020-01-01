package store

import (
	"bufio"
	"os"
)

const (
	wbufsize = 16384 * 4
)

type File struct {
	buf   *bufio.Writer
	fd    *os.File
	dirty bool
}

func (f *File) Write(buf []byte) (int, error) {
	if f.buf == nil {
		f.buf = bufio.NewWriterSize(f.fd, wbufsize)
	}
	f.dirty = true
	return f.buf.Write(buf)
}

func (f *File) WriteAt(buf []byte, off int64) (int, error) {
	if f.buf == nil {
		f.buf = bufio.NewWriterSize(f.fd, wbufsize)
	}
	f.dirty = true
	return f.fd.WriteAt(buf, off)
}

func (f *File) Read(buf []byte) (int, error) {
	return f.fd.Read(buf)
}

func (f *File) ReadAt(buf []byte, off int64) (int, error) {
	return f.fd.ReadAt(buf, off)
}

func (f *File) Commit() error {
	if f.dirty {
		if f.buf != nil {
			err := f.buf.Flush()
			if err != nil {
				return err
			}
		}
		err := f.fd.Sync()
		if err != nil {
			return err
		}
	}
	f.dirty = false
	return nil
}

func (f *File) Flush() error {
	return f.buf.Flush()
}

func (f *File) Close() error {
	return f.fd.Close()
}
