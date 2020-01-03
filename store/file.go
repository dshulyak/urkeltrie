package store

import (
	"github.com/spf13/afero"
)

type file struct {
	fd    afero.File
	dirty bool
}

func (f *file) Write(buf []byte) (int, error) {
	f.dirty = true
	return f.fd.Write(buf)
}

func (f *file) ReadAt(buf []byte, off int64) (int, error) {
	return f.fd.ReadAt(buf, off)
}

func (f *file) Commit() error {
	if f.dirty {
		err := f.fd.Sync()
		if err != nil {
			return err
		}
	}
	f.dirty = false
	return nil
}

func (f *file) Close() error {
	return f.fd.Close()
}
