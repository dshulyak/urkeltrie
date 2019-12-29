package urkeltrie

const (
	maxFileSize uint64 = 1 << 30
	VersionSize        = 32
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
	return &FileStore{
		dirtyTreeOffset:  new(Offset),
		dirtyValueOffset: new(Offset),
	}, nil
}

type FileStore struct {
	dir *Dir

	dirtyTreeOffset, dirtyValueOffset *Offset

	commitedTreeOffset, commitedValueOffset *Offset
	treeFiles                               map[uint64]*File
	valueFiles                              map[uint64]*File

	currentVersion uint64
	versions       map[uint64]*File
}

func (s *FileStore) TreeOffsetFor(size int) (uint64, uint64) {
	return s.dirtyTreeOffset.OffsetFor(size)
}

func (s *FileStore) ValueOffsetFor(size int) (uint64, uint64) {
	return s.dirtyValueOffset.OffsetFor(size)
}

func (s *FileStore) WriteValue(buf []byte) (int, error) {
	return 0, nil
}

func (s *FileStore) WriteTree(buf []byte) (int, error) {
	return 0, nil
}

func (s *FileStore) ReadTreeAt(idx, pos uint64, buf []byte) (int, error) {
	return 0, nil
}

func (s *FileStore) ReadValueAt(idx, pos uint64, buf []byte) (int, error) {
	return 0, nil
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
	return nil
}
