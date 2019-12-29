package urkeltrie

import (
	"os"
)

func OpenDir(path string) (*Dir, error) {
	err := os.MkdirAll(path, 0600)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}

	fd, err := os.OpenFile(path, 0600, os.ModeDir)
	if err != nil {
		return nil, err
	}
	return &Dir{
		fd: fd,
	}, nil
}

type Dir struct {
	fd    *os.File
	dirty bool
}

func (d *Dir) Commit() error {
	if d.dirty {
		err := d.fd.Sync()
		if err != nil {
			return err
		}
	}
	d.dirty = false
	return nil
}

func (d *Dir) Create(prefix string, index uint64) (*File, error) {
	d.dirty = true
	return nil, nil
}
