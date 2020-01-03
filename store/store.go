package store

import (
	"errors"

	"github.com/spf13/afero"
)

const (
	maxFileSize uint64 = 2 << 30

	versionPrefix = "version"
	treePrefix    = "tree"
	valuePrefix   = "value"
)

type Config struct {
	Path           string
	MaxFileSize    uint64
	WriteBuffer    int64 // per file
	ReadBufferSize int64 // per file
	MaxOpenedFiles int64
}

type Offset struct {
	index, offset uint64
	maxFileSize   uint64
}

func (o *Offset) OffsetFor(size int) (uint64, uint64) {
	usize := uint64(size)
	prev := o.offset
	if usize+o.offset > o.maxFileSize {
		o.index++
		o.offset = 0
		prev = 0
	}
	o.offset += usize
	return o.index, prev
}

func (o *Offset) Offset() (uint64, uint64) {
	return o.index, o.offset
}

func NewFileStore(path string) (*FileStore, error) {
	return NewFileStoreSize(path, maxFileSize)
}

func NewFileStoreSize(path string, fileSize uint64) (*FileStore, error) {
	var fs afero.Fs
	if len(path) > 0 {
		fs = afero.NewOsFs()
	} else {
		fs = afero.NewMemMapFs()
	}
	dir, err := OpenDir(fs, path)
	if err != nil {
		return nil, err
	}
	store := &FileStore{
		dir:              dir,
		fs:               fs,
		dirtyTreeOffset:  &Offset{maxFileSize: fileSize},
		dirtyValueOffset: &Offset{maxFileSize: fileSize},
		versionOffset:    &Offset{maxFileSize: fileSize},
		trees:            newGroup(treePrefix, dir, fileSize, 128<<20, true),
		values:           newGroup(valuePrefix, dir, fileSize, 128<<20, false),
	}
	return store, nil
}

type FileStore struct {
	fs afero.Fs

	dir *Dir

	// TODO if tree is discarded without commit or flush rewert offsets to non-dirty values
	// if after reload tree
	dirtyTreeOffset, dirtyValueOffset *Offset

	trees, values *filesGroup
	// TODO keep only last N (10000?) versions in a file
	versionOffset *Offset
	versions      *file
}

func (s *FileStore) getVersionFile() (*file, error) {
	if s.versions != nil {
		return s.versions, nil
	}
	f, err := s.dir.Open(versionPrefix, 0)
	if err != nil {
		return nil, err
	}
	s.versions = f
	return f, nil
}

func (s *FileStore) TreeOffsetFor(size int) (uint64, uint64) {
	return s.dirtyTreeOffset.OffsetFor(size)
}

func (s *FileStore) ValueOffsetFor(size int) (uint64, uint64) {
	return s.dirtyValueOffset.OffsetFor(size)
}

func (s *FileStore) WriteValue(buf []byte) (int, error) {
	return s.values.Write(buf)
}

func (s *FileStore) WriteTree(buf []byte) (int, error) {
	return s.trees.Write(buf)
}

func (s *FileStore) ReadTreeAt(index, off uint64, buf []byte) (int, error) {
	return s.trees.ReadAt(buf, index, off)
}

func (s *FileStore) ReadValueAt(index, off uint64, buf []byte) (int, error) {
	return s.values.ReadAt(buf, index, off)
}

func (s *FileStore) WriteVersion(buf []byte) (int, error) {
	s.versionOffset.OffsetFor(len(buf))
	f, err := s.getVersionFile()
	if err != nil {
		return 0, err
	}
	return f.Write(buf)
}

func (s *FileStore) ReadLastVersion(buf []byte) (int, error) {
	_, off := s.versionOffset.Offset()
	f, err := s.getVersionFile()
	if err != nil {
		return 0, err
	}
	return f.ReadAt(buf, int64(off)-int64(len(buf)))
}

func (s *FileStore) ReadVersion(version uint64, buf []byte) (int, error) {
	if version == 0 {
		return 0, errors.New("version 0 not found")
	}
	off := (version - 1) * uint64(len(buf))
	f, err := s.getVersionFile()
	if err != nil {
		return 0, err
	}
	return f.ReadAt(buf, int64(off))
}

func (s *FileStore) Commit() error {
	err := s.dir.Commit()
	if err != nil {
		return err
	}
	if err := s.trees.Commit(); err != nil {
		return err
	}
	if err := s.values.Commit(); err != nil {
		return err
	}
	f, err := s.getVersionFile()
	if err != nil {
		return err
	}
	return f.Commit()
}

func (s *FileStore) Flush() error {
	if err := s.trees.Flush(); err != nil {
		return err
	}
	if err := s.values.Flush(); err != nil {
		return err
	}
	return nil
}

func (s *FileStore) Close() error {
	if err := s.trees.Close(); err != nil {
		return err
	}
	if err := s.values.Close(); err != nil {
		return err
	}
	if s.versions != nil {
		err := s.versions.Close()
		if err != nil {
			return err
		}
	}
	return s.dir.Close()
}
