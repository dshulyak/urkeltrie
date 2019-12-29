package urkeltrie

import "errors"

func bitSet(key [32]byte, index int) bool {
	pos, bit := index/8, index%8
	return (key[pos] & (1 << bit)) > 0
}

func createLeaf(store *FileStore, idx, pos uint64) *leaf {
	return &leaf{
		store: store,
		pos:   pos,
		idx:   idx,
	}
}

func newLeaf(store *FileStore, key [size]byte, value []byte) *leaf {
	return &leaf{
		store:       store,
		dirty:       true,
		key:         key,
		value:       value,
		valueLength: len(value),
	}
}

type leaf struct {
	store         *FileStore
	dirty, synced bool

	idx, pos uint64

	key, hash   [size]byte
	value       []byte
	valueLength int

	valueIdx, valuePos uint64
}

func (l *leaf) presync() error {
	if l.dirty {
		l.idx, l.pos = l.store.TreeOffsetFor(l.Size())
	} else {
		buf := make([]byte, l.Size())
		_, err := l.store.ReadTreeAt(l.idx, l.pos, buf)
		if err != nil {
			return err
		}
		l.Unmarshal(buf)
		l.value = make([]byte, l.valueLength)
		_, err = l.store.ReadValueAt(l.valueIdx, l.valuePos, l.value)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l *leaf) Pos() uint64 {
	return l.pos
}

func (l *leaf) Idx() uint64 {
	return l.idx
}

func (l *leaf) Put(key [32]byte, value []byte) error {
	if !l.synced {
		err := l.presync()
		if err != nil {
			return nil
		}
		l.synced = true
	}
	// overwrite will create new branch. old version will be still accessible using previous root
	if l.key == key {
		l.hash = zeros
		l.value = value
		if !l.dirty {
			l.dirty = true
			_ = l.presync()
		}
	}
	return nil
}

func (l *leaf) Get(key [32]byte) ([]byte, error) {
	if !l.synced {
		err := l.presync()
		if err != nil {
			return nil, err
		}
		l.synced = true
	}
	if l.key == key {
		return l.value, nil
	}
	return nil, errors.New("not found")
}

func (l *leaf) Hash() (rst [size]byte) {
	if l.hash != zeros {
		return l.hash
	}
	l.hash = leafHash(l.key, l.value)
	return l.hash
}

func (l *leaf) Size() int {
	return leafSize
}

func (l *leaf) Marshal() []byte {
	buf := make([]byte, l.Size())
	l.MarshalTo(buf)
	return buf
}

func (l *leaf) MarshalTo(buf []byte) {
	_ = buf[l.Size()-1]
	idx := 0
	copy(buf[idx:], l.key[:])
	idx += 32
	order.PutUint64(buf[idx:], l.valueIdx)
	idx += 8
	order.PutUint64(buf[idx:], l.valuePos)
	idx += 8
	order.PutUint64(buf[idx:], uint64(len(l.value)))
}

func (l *leaf) Unmarshal(buf []byte) {
	_ = buf[l.Size()-1]
	copy(l.key[:], buf)
	l.valueIdx = order.Uint64(buf[32:])
	l.valuePos = order.Uint64(buf[40:])
	l.valueLength = int(order.Uint64(buf[48:]))
}

func (l *leaf) Commit() error {
	if !l.dirty {
		return nil
	}
	idx, pos := l.store.ValueOffsetFor(len(l.value))
	n, err := l.store.WriteValue(l.value)
	if err != nil {
		return err
	}
	if n != len(l.value) {
		return errors.New("partial value write")
	}
	l.valueIdx = idx
	l.valuePos = pos
	n, err = l.store.WriteTree(l.Marshal())
	if err != nil {
		return err
	}
	if n != l.Size() {
		return errors.New("partial tree write")
	}
	l.dirty = false
	return nil
}

func (l *leaf) Prove(key [size]byte, proof *Proof) error {
	return nil
}

func leafHash(key [size]byte, value []byte) (rst [size]byte) {
	h := hasher()
	h.Write([]byte{leafDomain})
	h.Write(key[:])
	h.Write(value)
	h.Sum(rst[:0])
	return rst
}
