package store

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"

	"github.com/spf13/afero"
)

func OpenDir(fs afero.Fs, path string) (*Dir, error) {
	err := fs.MkdirAll(path, 0700)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}

	fd, err := fs.OpenFile(path, 0600, os.ModeDir)
	if err != nil {
		return nil, err
	}
	return &Dir{
		fs: fs,
		fd: fd,
	}, nil
}

type Dir struct {
	fs    afero.Fs
	fd    afero.File
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

func (d *Dir) Open(prefix string, index uint32) (*file, error) {
	d.dirty = true
	path := filepath.Join(d.fd.Name(), fmt.Sprintf("%s-%d.%s", prefix, index, dbformat))
	fd, err := d.fs.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil && !os.IsExist(err) {
		return nil, err
	}
	return &file{fd: fd}, nil
}

func (d *Dir) LastIndex(prefix string) (uint32, error) {
	names, err := d.fd.Readdirnames(-1)
	if err != nil {
		return 0, err
	}
	var (
		max  uint32
		expr = regexp.MustCompile(fmt.Sprintf("(?:%s-)([0-9]+)", prefix))
	)
	for _, name := range names {
		matches := expr.FindStringSubmatch(name)
		if len(matches) > 1 {
			idx64, err := strconv.ParseUint(matches[1], 10, 32)
			if err != nil {
				return 0, fmt.Errorf("inccorect file forat: %w", err)
			}
			idx := uint32(idx64)
			if idx > max {
				max = idx
			}
		}
	}
	return max, nil
}

func (d *Dir) Close() error {
	return d.fd.Close()
}
