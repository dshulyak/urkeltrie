package store

func newOffset(index, offset, fileSize uint32) *Offset {
	return &Offset{
		index:       index,
		offset:      offset,
		maxFileSize: fileSize,
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
