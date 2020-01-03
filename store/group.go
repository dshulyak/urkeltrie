package store

func newGroup(prefix string, dir *Dir, fileSize uint64, bufSize int, readCacheWrap bool) *filesGroup {
	return &filesGroup{
		groupPrefix:   prefix,
		dir:           dir,
		readCacheWrap: readCacheWrap,
		bufSize:       bufSize,
		offset:        &Offset{maxFileSize: fileSize},
		readers:       map[uint64]reader{},
		opened:        map[uint64]*file{},
	}
}

type reader interface {
	ReadAt([]byte, int64) (int, error)
}

type writer interface {
	Write([]byte) (int, error)
	Commit() error
	Flush() error
	Reset()
}

type filesGroup struct {
	groupPrefix string
	dir         *Dir

	readCacheWrap bool

	bufSize int
	windex  uint64
	writer  writer
	// list of writers that need to be reset after commit
	dirty []writer

	offset *Offset

	opened map[uint64]*file

	readers map[uint64]reader
}

func (fg *filesGroup) get(index uint64) (*file, error) {
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

func (fg *filesGroup) reader(index uint64) (reader, error) {
	r, exist := fg.readers[index]
	if exist {
		return r, nil
	}
	f, err := fg.get(index)
	if err != nil {
		return nil, err
	}
	if fg.readCacheWrap {
		fg.readers[index] = NewCachingFile(f)
	} else {
		fg.readers[index] = f
	}
	return fg.readers[index], nil
}

func (fg *filesGroup) getWriter(index uint64) (writer, error) {
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

func (fg *filesGroup) Write(buf []byte) (int, error) {
	index, _ := fg.offset.OffsetFor(len(buf))
	w, err := fg.getWriter(index)
	if err != nil {
		return 0, err
	}
	return w.Write(buf)
}

func (fg *filesGroup) ReadAt(buf []byte, index, off uint64) (int, error) {
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
