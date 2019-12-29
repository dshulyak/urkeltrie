package urkeltrie

const (
	maxFileSize uint64 = 1 << 30
	VersionSize        = 32

	versionPrefix = "version"
	treePrefix    = "tree"
	valuePrefix   = "value"
)

type Offset struct {
	index, offset uint64
}

func (o *Offset) OffsetFor(size int) (uint64, uint64) {
	usize := uint64(size)
	prev := o.offset
	if usize+o.offset > maxFileSize {
		o.index++
		o.offset = 0
		prev = 0
	}
	o.offset += usize
	return o.index, prev
}

func NewFileStore(path string) (*FileStore, error) {
	store := &FileStore{
		dirtyTreeOffset:  new(Offset),
		dirtyValueOffset: new(Offset),
		treeOffset:       new(Offset),
		valueOffset:      new(Offset),
		treeFiles:        map[uint64]*File{},
		valueFiles:       map[uint64]*File{},
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

	dirtyTreeOffset, dirtyValueOffset *Offset

	treeOffset, valueOffset *Offset
	treeFiles               map[uint64]*File
	valueFiles              map[uint64]*File

	versionOffset uint64
	versions      map[uint64]*File
}

func (s *FileStore) getValueFile(index uint64) (*File, error) {
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

func (s *FileStore) getTreeFile(index uint64) (*File, error) {
	f, exist := s.treeFiles[index]
	if exist {
		return f, nil
	}
	f, err := s.dir.Open(treePrefix, index)
	if err != nil {
		return nil, err
	}
	s.treeFiles[index] = f
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

func (s *FileStore) WriteVersion(index uint64, version [VersionSize]byte) (int, error) {
	return 0, nil
}

func (s *FileStore) LastVersion() (rst [VersionSize]byte, err error) {
	return
}

func (s *FileStore) GetVersion(index uint64) (rst [VersionSize]byte, err error) {
	return
}

func (s *FileStore) Commit() error {
	err := s.dir.Commit()
	if err != nil {
		return err
	}
	for _, f := range s.valueFiles {
		err = f.Commit()
		if err != nil {
			return err
		}
	}
	for _, f := range s.treeFiles {
		err = f.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}
