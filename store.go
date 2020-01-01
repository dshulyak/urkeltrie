package urkeltrie

import (
	"errors"
)

const (
	maxFileSize uint64 = 2 << 30

	versionPrefix = "version"
	treePrefix    = "tree"
	valuePrefix   = "value"
)

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
	return newFileStore(path, maxFileSize)
}

type storeRW interface {
	Write([]byte) (int, error)
	ReadAt([]byte, int64) (int, error)
	Commit() error
	Flush() error
	Close() error
}

func newFileStore(path string, fileSize uint64) (*FileStore, error) {
	store := &FileStore{
		dirtyTreeOffset:  &Offset{maxFileSize: fileSize},
		dirtyValueOffset: &Offset{maxFileSize: fileSize},
		treeOffset:       &Offset{maxFileSize: fileSize},
		valueOffset:      &Offset{maxFileSize: fileSize},
		versionOffset:    &Offset{maxFileSize: fileSize},
		treeFiles:        map[uint64]storeRW{},
		valueFiles:       map[uint64]storeRW{},
	}
	if len(path) > 0 {
		dir, err := OpenDir(path)
		if err != nil {
			return nil, err
		}
		store.dir = dir
	}
	return store, nil
}

type FileStore struct {
	dir *Dir

	// TODO if tree is discarded without commit or flush rewert offsets to non-dirty values
	// if after reload tree
	dirtyTreeOffset, dirtyValueOffset *Offset

	// TODO if write failed - reload tree state from disk
	treeOffset, valueOffset, versionOffset *Offset
	// TODO move files management to Dir
	// TODO add max open files limitation
	treeFiles  map[uint64]storeRW
	valueFiles map[uint64]storeRW
	// TODO keep only last N (10000?) versions in a file
	versions storeRW
}

func (s *FileStore) getValueFile(index uint64) (storeRW, error) {
	f, exist := s.valueFiles[index]
	if exist {
		return f, nil
	}
	f, err := s.dir.Open(valuePrefix, index)
	if err != nil {
		return nil, err
	}
	s.valueFiles[index] = f
	return f, nil
}

func (s *FileStore) getTreeFile(index uint64) (storeRW, error) {
	f, exist := s.treeFiles[index]
	if exist {
		return f, nil
	}
	nf, err := s.dir.Open(treePrefix, index)
	if err != nil {
		return nil, err
	}
	cf := NewCachingFile(nf)
	s.treeFiles[index] = cf
	return cf, nil
}

func (s *FileStore) getVersionFile() (storeRW, error) {
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
	index, _ := s.valueOffset.OffsetFor(len(buf))
	f, err := s.getValueFile(index)
	if err != nil {
		return 0, err
	}
	return f.Write(buf)
}

func (s *FileStore) WriteTree(buf []byte) (int, error) {
	index, _ := s.treeOffset.OffsetFor(len(buf))
	f, err := s.getTreeFile(index)
	if err != nil {
		return 0, err
	}
	return f.Write(buf)
}

func (s *FileStore) ReadTreeAt(index, off uint64, buf []byte) (int, error) {
	f, err := s.getTreeFile(index)
	if err != nil {
		return 0, err
	}
	// FIXME replace off with uint32 and index with uint16
	return f.ReadAt(buf, int64(off))
}

func (s *FileStore) ReadValueAt(index, off uint64, buf []byte) (int, error) {
	f, err := s.getValueFile(index)
	if err != nil {
		return 0, err
	}
	// FIXME replace every uint64 with uint32
	return f.ReadAt(buf, int64(off))
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
	return f.ReadAt(buf, int64(off)-versionSize)
}

func (s *FileStore) ReadVersion(version uint64, buf []byte) (int, error) {
	if version == 0 {
		return 0, errors.New("version 0 not found")
	}
	off := (version - 1) * versionSize
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
	for _, f := range s.valueFiles {
		err := f.Commit()
		if err != nil {
			return err
		}
	}
	for _, f := range s.treeFiles {
		err := f.Commit()
		if err != nil {
			return err
		}
	}
	f, err := s.getVersionFile()
	if err != nil {
		return err
	}
	return f.Commit()
}

func (s *FileStore) Flush() error {
	for _, f := range s.valueFiles {
		err := f.Flush()
		if err != nil {
			return err
		}
	}
	for _, f := range s.treeFiles {
		err := f.Flush()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *FileStore) Close() error {
	for _, f := range s.valueFiles {
		err := f.Close()
		if err != nil {
			return err
		}
	}
	for _, f := range s.treeFiles {
		err := f.Close()
		if err != nil {
			return err
		}
	}
	if s.versions != nil {
		err := s.versions.Close()
		if err != nil {
			return err
		}
	}
	return s.dir.Close()
}
