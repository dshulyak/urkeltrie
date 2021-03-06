package store

import "sync"

func newGroup(prefix string, dir *Dir, fileSize uint32, bufSize int) *filesGroup {
	return &filesGroup{
		maxFileSize: fileSize,
		groupPrefix: prefix,
		dir:         dir,
		bufSize:     bufSize,
		dirtyOffset: &Offset{maxFileSize: fileSize},
		offset:      &Offset{maxFileSize: fileSize},
		readers:     map[uint32]reader{},
		opened:      map[uint32]*file{},
	}
}

type reader interface {
	ReadAt([]byte, int64) (int, error)
	ReadStats(*GroupStats)
}

type writer interface {
	Write([]byte) (int, error)
	Commit() error
	Flush() error
	Reset()
	ReadStats(*GroupStats)
}

type filesGroup struct {
	groupPrefix string
	dir         *Dir

	maxFileSize uint32

	bufSize int
	windex  uint32
	writer  writer
	// list of writers that need to be reset after commit
	dirty []writer

	dirtyOffset *Offset
	offset      *Offset

	omu    sync.Mutex
	opened map[uint32]*file

	rmu     sync.Mutex
	readers map[uint32]reader
}

func (fg *filesGroup) restore() error {
	last, err := fg.dir.LastIndex(fg.groupPrefix)
	if err != nil {
		return err
	}
	f, err := fg.dir.Open(fg.groupPrefix, last)
	if err != nil {
		return err
	}
	size, err := f.Size()
	if err != nil {
		return err
	}
	fg.offset = newOffset(last, uint32(size), fg.maxFileSize)
	fg.dirtyOffset = newOffset(last, uint32(size), fg.maxFileSize)
	fg.opened[last] = f
	return nil
}

func (fg *filesGroup) get(index uint32) (*file, error) {
	fg.omu.Lock()
	defer fg.omu.Unlock()
	f, opened := fg.opened[index]
	if opened {
		return f, nil
	}
	f, err := fg.dir.Open(fg.groupPrefix, index)
	if err != nil {
		return nil, err
	}
	fg.opened[index] = f
	return f, nil
}

func (fg *filesGroup) reader(index uint32) (reader, error) {
	fg.rmu.Lock()
	defer fg.rmu.Unlock()
	r, exist := fg.readers[index]
	if exist {
		return r, nil
	}
	f, err := fg.get(index)
	if err != nil {
		return nil, err
	}
	fg.readers[index] = f
	return f, nil
}

func (fg *filesGroup) getWriter(index uint32) (writer, error) {
	if fg.writer != nil && index == fg.windex {
		return fg.writer, nil
	}
	f, err := fg.get(index)
	if err != nil {
		return nil, err
	}
	if fg.writer == nil {
		fg.writer = newBuffered(f, fg.bufSize)
	} else {
		fg.dirty = append(fg.dirty, fg.writer)
		fg.writer = newBuffered(f, fg.bufSize)
		fg.windex = index
	}
	return fg.writer, nil
}

func (fg *filesGroup) AllocateOffset(size int) (uint32, uint32) {
	return fg.dirtyOffset.OffsetFor(size)
}

func (fg *filesGroup) Write(buf []byte) (int, error) {
	index, _ := fg.offset.OffsetFor(len(buf))
	w, err := fg.getWriter(index)
	if err != nil {
		return 0, err
	}
	return w.Write(buf)
}

func (fg *filesGroup) ReadAt(buf []byte, index, off uint32) (int, error) {
	f, err := fg.reader(index)
	if err != nil {
		return 0, err
	}
	return f.ReadAt(buf, int64(off))
}

func (fg *filesGroup) Flush() error {
	if fg.writer != nil {
		if err := fg.writer.Flush(); err != nil {
			return err
		}
	}
	for _, w := range fg.dirty {
		if err := w.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (fg *filesGroup) Commit() error {
	if fg.writer != nil {
		if err := fg.writer.Commit(); err != nil {
			return err
		}
	}
	for _, w := range fg.dirty {
		if err := w.Commit(); err != nil {
			return err
		}
		w.Reset()
	}
	fg.dirty = nil
	return nil
}

func (fg *filesGroup) Close() error {
	for _, f := range fg.opened {
		if err := f.Close(); err != nil {
			return err
		}
	}
	fg.writer = nil
	fg.dirty = nil
	fg.readers = nil
	return nil
}

func (fg *filesGroup) ReadStats(stats *GroupStats) {
	if fg.writer != nil {
		fg.writer.ReadStats(stats)
	}
	for _, w := range fg.dirty {
		w.ReadStats(stats)
	}
	for _, r := range fg.readers {
		r.ReadStats(stats)
	}
	stats.MeanFlushSize = stats.FlushSize / stats.FlushCount
	stats.FlushUtilization = float64(stats.MeanFlushSize) / float64(fg.bufSize)
	stats.DiskSize = uint64(fg.offset.Size())
}
