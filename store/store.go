package store

import (
	"errors"

	"github.com/spf13/afero"
)

const (
	maxFileSize uint32 = 2 << 30

	versionPrefix = "version"
	treePrefix    = "tree"
	valuePrefix   = "value"
	dbformat      = "udb"
)

type GroupStats struct {
	DiskSize              uint64
	CacheHit, CacheMiss   uint64
	FlushSize, FlushCount uint64
	MeanFlushSize         uint64
	FlushUtilization      float64 // mean flush size / buffer size
}

type Stats struct {
	Tree, Value GroupStats
	DiskSize    uint64
}

type Config struct {
	Path                string
	MaxFileSize         uint32
	TreeWriteBuffer     int
	ValueWriteBuffer    int
	ReadBufferChunkSize int
}

func DevConfig(path string) Config {
	return Config{
		Path:                path,
		MaxFileSize:         maxFileSize,
		TreeWriteBuffer:     1 << 10,
		ValueWriteBuffer:    1 << 10,
		ReadBufferChunkSize: 1024,
	}
}

func DefaultProdConfig(path string) Config {
	return Config{
		Path:                path,
		MaxFileSize:         maxFileSize,
		TreeWriteBuffer:     16 << 20,
		ValueWriteBuffer:    8 << 20,
		ReadBufferChunkSize: 1024,
	}
}

type Offset struct {
	index, offset uint32
	maxFileSize   uint32
}

func (o *Offset) OffsetFor(size int) (uint32, uint32) {
	usize := uint32(size)
	prev := o.offset
	if usize+o.offset > o.maxFileSize {
		o.index++
		o.offset = 0
		prev = 0
	}
	o.offset += usize
	return o.index, prev
}

func (o *Offset) Offset() (uint32, uint32) {
	return o.index, o.offset
}

func (o *Offset) Size() uint64 {
	return uint64(o.index)*uint64(o.maxFileSize) + uint64(o.offset)
}

func NewFileStore(conf Config) (*FileStore, error) {
	var fs afero.Fs
	if len(conf.Path) > 0 {
		fs = afero.NewOsFs()
	} else {
		fs = afero.NewMemMapFs()
	}
	dir, err := OpenDir(fs, conf.Path)
	if err != nil {
		return nil, err
	}
	store := &FileStore{
		conf:             conf,
		dir:              dir,
		fs:               fs,
		dirtyTreeOffset:  &Offset{maxFileSize: conf.MaxFileSize},
		dirtyValueOffset: &Offset{maxFileSize: conf.MaxFileSize},
		versionOffset:    &Offset{maxFileSize: conf.MaxFileSize},
		trees:            newGroup(treePrefix, dir, conf.MaxFileSize, conf.TreeWriteBuffer, conf.ReadBufferChunkSize),
		values:           newGroup(valuePrefix, dir, conf.MaxFileSize, conf.ValueWriteBuffer, conf.ReadBufferChunkSize),
	}
	return store, nil
}

type FileStore struct {
	fs   afero.Fs
	conf Config

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

func (s *FileStore) TreeOffsetFor(size int) (uint32, uint32) {
	return s.dirtyTreeOffset.OffsetFor(size)
}

func (s *FileStore) ValueOffsetFor(size int) (uint32, uint32) {
	return s.dirtyValueOffset.OffsetFor(size)
}

func (s *FileStore) WriteValue(buf []byte) (int, error) {
	return s.values.Write(buf)
}

func (s *FileStore) WriteTree(buf []byte) (int, error) {
	return s.trees.Write(buf)
}

func (s *FileStore) ReadTreeAt(index, off uint32, buf []byte) (int, error) {
	return s.trees.ReadAt(buf, index, off)
}

func (s *FileStore) ReadValueAt(index, off uint32, buf []byte) (int, error) {
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

func (s *FileStore) ReadStats(stats *Stats) {
	s.trees.ReadStats(&stats.Tree)
	s.values.ReadStats(&stats.Value)
	stats.DiskSize = stats.Tree.DiskSize + stats.Value.DiskSize + s.versionOffset.Size()
}
