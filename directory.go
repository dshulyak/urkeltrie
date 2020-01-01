package urkeltrie

import (
	"os"
	"path/filepath"
	"strconv"
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

func (d *Dir) Open(prefix string, index uint64) (*File, error) {
	d.dirty = true
	path := filepath.Join(d.fd.Name(), prefix+strconv.FormatUint(index, 10))
	fd, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}
	return &File{fd: fd}, nil
}

func (d *Dir) Close() error {
	return d.fd.Close()
}
