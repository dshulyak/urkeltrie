package urkeltrie

import (
	"errors"
	"fmt"

	"github.com/dshulyak/urkeltrie/store"
)

func bitSet(key [32]byte, index int) bool {
	pos, bit := index/8, index%8
	return (key[pos] & (1 << bit)) > 0
}

func createLeaf(store *store.FileStore, idx, pos uint32, hash []byte) *leaf {
	return &leaf{
		store: store,
		pos:   pos,
		idx:   idx,
		hash:  hash,
	}
}

func newLeaf(store *store.FileStore, key [size]byte, value []byte) *leaf {
	return &leaf{
		store:       store,
		dirty:       true,
		key:         key,
		value:       value,
		valueLength: len(value),
	}
}

type leaf struct {
	store         *store.FileStore
	dirty, synced bool

	idx, pos uint32

	key         [size]byte
	hash        []byte
	value       []byte
	valueLength int

	valueIdx, valuePos uint32
}

func (l *leaf) Sync() error {
	return l.sync()
}

func (l *leaf) isDirty() bool {
	return l.dirty
}

func (l *leaf) sync() error {
	if !l.synced && !l.dirty {
		buf := make([]byte, l.Size())
		n, err := l.store.ReadTreeAt(l.idx, l.pos, buf)
		if err != nil {
			return fmt.Errorf("failed to load leaf node at %d:%d. read %d bytes. error %w", l.idx, l.pos, n, err)
		}
		if err := l.Unmarshal(buf); err != nil {
			return err
		}
		l.value = make([]byte, l.valueLength+4)
		_, err = l.store.ReadValueAt(l.valueIdx, l.valuePos, l.value)
		if err != nil {
			return fmt.Errorf("failed to load value at %d:%d. error %w", l.valueIdx, l.valuePos, err)
		}
		if crcSum32(l.value[:l.valueLength]) != order.Uint32(l.value[l.valueLength:]) {
			return fmt.Errorf("%w: leaf value corrupted", ErrCRC)
		}
		l.value = l.value[:l.valueLength]
		l.synced = true
	}
	return nil
}

func (l *leaf) Position() (uint32, uint32) {
	return l.idx, l.pos
}

func (l *leaf) Put(key [32]byte, value []byte) error {
	if err := l.sync(); err != nil {
		return err
	}
	if lth := len(value); lth > maxValueSize {
		return fmt.Errorf("value is longer then max allower, %d > %d", lth, maxValueSize)
	}
	// overwrite will create new branch. old version will be still accessible using previous root
	if l.key == key {
		l.hash = nil
		l.value = value
		l.dirty = true
	}
	return nil
}

func (l *leaf) Delete(key [size]byte) (bool, error) {
	if err := l.sync(); err != nil {
		return false, err
	}
	return l.key == key, nil
}

func (l *leaf) Get(key [size]byte) ([]byte, error) {
	if err := l.sync(); err != nil {
		return nil, err
	}
	if l.key == key {
		return l.value, nil
	}
	return nil, fmt.Errorf("%w: collision, key %x not found", ErrNotFound, key)
}

func (l *leaf) Hash() []byte {
	if l.hash != nil {
		return l.hash
	}
	l.hash = leafHash(l.key[:], l.value)
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

func (l *leaf) Allocate() {
	if l.dirty {
		l.idx, l.pos = l.store.TreeOffsetFor(l.Size())
	}
}

func (l *leaf) MarshalTo(buf []byte) {
	_ = buf[l.Size()-1]
	copy(buf[:], l.key[:])
	order.PutUint32(buf[32:], l.valueIdx)
	order.PutUint32(buf[36:], l.valuePos)
	order.PutUint32(buf[40:], uint32(len(l.value)))
	appendCrcSum32(buf[:44], buf[44:44])
}

func (l *leaf) Unmarshal(buf []byte) error {
	_ = buf[l.Size()-1]
	if crcSum32(buf[:44]) != order.Uint32(buf[44:]) {
		return ErrCRC
	}
	copy(l.key[:], buf)
	l.valueIdx = order.Uint32(buf[32:])
	l.valuePos = order.Uint32(buf[36:])
	l.valueLength = int(order.Uint32(buf[40:]))
	return nil
}

func (l *leaf) Commit() error {
	if !l.dirty {
		return nil
	}
	idx, pos := l.store.ValueOffsetFor(len(l.value) + 4)
	n, err := l.store.WriteValue(l.value)
	if err != nil {
		return err
	}
	if n != len(l.value) {
		return errors.New("partial value write")
	}

	// don't worry about small write, writes buffered internally
	crc := make([]byte, 4)
	appendCrcSum32(l.value, crc[:0])
	n, err = l.store.WriteValue(crc)
	if err != nil {
		return err
	}
	if n != 4 {
		return errors.New("crc wasn't fully writte")
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
	if err := l.sync(); err != nil {
		return err
	}
	if l.key == key {
		proof.addValue(l.value)
		return nil
	}
	proof.addCollision(l.key[:], l.value)
	return nil
}

func leafHash(hkey, value []byte) []byte {
	rst := make([]byte, 0, 32)
	h := hasher()
	h.Write([]byte{leafDomain})
	h.Write(hkey)
	h.Write(value)
	rst = h.Sum(rst)
	return rst
}
