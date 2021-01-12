package urkeltrie

import (
	"errors"
	"fmt"

	"github.com/dshulyak/urkeltrie/store"
)

func bitSet(key [32]byte, index uint8) bool {
	pos, bit := index/8, index%8
	return (key[pos] & (1 << bit)) > 0
}

func createLeaf(idx, pos uint32, hash []byte) *leaf {
	return &leaf{
		pos:  pos,
		idx:  idx,
		hash: hash,
	}
}

func newLeaf(key [size]byte, preimage, value []byte) *leaf {
	return &leaf{
		dirty:       true,
		key:         key,
		preimage:    preimage,
		value:       value,
		valueLength: len(value),
	}
}

type leaf struct {
	dirty, synced bool

	idx, pos uint32

	preimage    []byte
	key         [size]byte
	hash        []byte
	value       []byte
	keyLength   int
	valueLength int

	valueIdx, valuePos uint32
}

func (l *leaf) Sync(store *store.FileStore) error {
	return l.sync(store)
}

func (l *leaf) isDirty() bool {
	return l.dirty
}

func (l *leaf) sync(store *store.FileStore) error {
	if !l.synced && !l.dirty {
		buf := make([]byte, l.Size())

		n, err := store.ReadTreeAt(l.idx, l.pos, buf)
		if err != nil {
			return fmt.Errorf("failed to load leaf node at %d:%d. read %d bytes. error %w", l.idx, l.pos, n, err)
		}
		if err := l.Unmarshal(buf); err != nil {
			return err
		}
		body := make([]byte, l.keyLength+l.valueLength+4)
		_, err = store.ReadValueAt(l.valueIdx, l.valuePos, body)
		if err != nil {
			return fmt.Errorf("failed to load value at %d:%d. error %w", l.valueIdx, l.valuePos, err)
		}

		if crcSum32(body[:l.keyLength+l.valueLength]) != order.Uint32(body[l.keyLength+l.valueLength:]) {
			return fmt.Errorf("%w: leaf value corrupted", ErrCRC)
		}
		l.preimage = body[:l.keyLength]
		l.value = body[l.keyLength : l.keyLength+l.valueLength]
		l.synced = true
	}
	return nil
}

func (l *leaf) Position() (uint32, uint32) {
	return l.idx, l.pos
}

func (l *leaf) Put(store *store.FileStore, key [32]byte, value []byte) error {
	if err := l.sync(store); err != nil {
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

func (l *leaf) Delete(store *store.FileStore, key [size]byte) (bool, bool, error) {
	if err := l.sync(store); err != nil {
		return false, false, err
	}
	match := l.key == key
	return match, match, nil
}

func (l *leaf) Get(store *store.FileStore, key [size]byte) ([]byte, error) {
	if err := l.sync(store); err != nil {
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
	rst := sum(l.value)
	l.hash = leafHash(l.key[:], rst[:])
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

func (l *leaf) Allocate(store *store.FileStore) {
	if l.dirty {
		l.idx, l.pos = store.TreeOffsetFor(l.Size())
	}
}

func (l *leaf) MarshalTo(buf []byte) {
	_ = buf[l.Size()-1]
	copy(buf[:], l.key[:])
	order.PutUint32(buf[32:], l.valueIdx)
	order.PutUint32(buf[36:], l.valuePos)
	order.PutUint32(buf[40:], uint32(len(l.preimage)))
	order.PutUint32(buf[44:], uint32(len(l.value)))
	putCrcSum32(buf[48:52], buf[:48])
}

func (l *leaf) Unmarshal(buf []byte) error {
	_ = buf[l.Size()-1]
	if crcSum32(buf[:48]) != order.Uint32(buf[48:]) {
		return ErrCRC
	}
	copy(l.key[:], buf)
	l.valueIdx = order.Uint32(buf[32:])
	l.valuePos = order.Uint32(buf[36:])
	l.keyLength = int(order.Uint32(buf[40:]))
	l.valueLength = int(order.Uint32(buf[44:]))
	return nil
}

func (l *leaf) Commit(store *store.FileStore) error {
	if !l.dirty {
		return nil
	}
	idx, pos := store.ValueOffsetFor(len(l.preimage) + len(l.value) + 4)

	bodylth := len(l.preimage) + len(l.value)
	buf := make([]byte, len(l.preimage)+len(l.value)+4)
	copy(buf, l.preimage)
	copy(buf[len(l.preimage):], l.value)
	putCrcSum32(buf[bodylth:bodylth+4], buf[:bodylth])
	n, err := store.WriteValue(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return errors.New("partial leaf body write")
	}

	l.valueIdx = idx
	l.valuePos = pos
	n, err = store.WriteTree(l.Marshal())
	if err != nil {
		return err
	}
	if n != l.Size() {
		return errors.New("partial tree write")
	}
	l.dirty = false
	return nil
}

func (l *leaf) Prove(store *store.FileStore, key [size]byte, proof *Proof) error {
	if err := l.sync(store); err != nil {
		return err
	}
	if l.key == key {
		proof.addValue(l.value)
		return nil
	}
	rst := sum(l.value)
	proof.addCollision(l.key[:], rst[:])
	return nil
}

func (l *leaf) makeEntry(store *store.FileStore) (Entry, error) {
	if err := l.sync(store); err != nil {
		return nil, err
	}
	return entry{
		key:   l.preimage,
		value: l.value,
	}, nil
}

type entry struct {
	key, value []byte
}

func (e entry) Key() ([]byte, error) {
	return e.key, nil
}

func (e entry) Value() ([]byte, error) {
	return e.value, nil
}

func leafHash(hkey, hvalue []byte) []byte {
	rst := make([]byte, 0, 32)
	h := hasher()
	h.Write([]byte{leafDomain})
	h.Write(hkey)
	h.Write(hvalue)
	rst = h.Sum(rst)
	return rst
}
