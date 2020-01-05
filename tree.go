package urkeltrie

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"sync"

	"github.com/dshulyak/urkeltrie/store"
)

const (
	size    = 32
	lastBit = size*8 - 1

	leafDomain  = 0x01
	innerDomain = 0x02

	leafSize     = 32 + 4 + 4 + 4 + 4 + 4   // key (hash), value idx, value pos, key length, value length, crc
	innerSize    = 2 + 2*4 + 2*4 + 2*32 + 4 // node type x 2, leaf idx x 2, leaf pos x 2, leaf hashses x 2, crc
	versionSize  = 8 + 4 + 4 + 32 + 4       // version, idx, pos, hash, crc
	maxValueSize = int(^uint32(0))
)

var (
	zeros, zerosHash [size]byte
	order            = binary.BigEndian

	digestPool = sync.Pool{New: func() interface{} { return hasher() }}
	innerPool  = sync.Pool{New: func() interface{} { return make([]byte, innerSize) }}
	// results used for async hash computation
	results = sync.Pool{New: func() interface{} { return make(chan []byte, 1) }}

	crcTable = crc32.MakeTable(crc32.Castagnoli)
)

func init() {
	h := hasher()
	h.Write([]byte{leafDomain})
	h.Write(zeros[:])
	tmp := zerosHash[:0]
	h.Sum(tmp)
}

func NewTree(store *store.FileStore) *Tree {
	return &Tree{store: store}
}

type Tree struct {
	store *store.FileStore

	version uint64
	root    *inner
}

func (t *Tree) Iterate(iterf IterateFunc) error {
	if t.root == nil {
		return nil
	}
	_, err := t.root.iterate(false, iterf)
	return err
}

func (t *Tree) ReverseIterate(iterf IterateFunc) error {
	if t.root == nil {
		return nil
	}
	_, err := t.root.iterate(true, iterf)
	return err
}

func (t *Tree) Get(key []byte) ([]byte, error) {
	return t.GetRaw(sum(key))
}

func (t *Tree) GetRaw(key [size]byte) ([]byte, error) {
	if t.root == nil {
		return nil, errors.New("not found")
	}
	return t.root.Get(key)
}

func (t *Tree) Version() uint64 {
	return t.version
}

func (t *Tree) Put(key, value []byte) error {
	return t.PutRaw(sum(key), key, value)
}

func (t *Tree) PutRaw(key [size]byte, preimage, value []byte) error {
	if t.root == nil {
		t.root = newInner(t.store, 0)
	}
	leaf := newLeaf(t.store, key, preimage, value)
	return t.root.Insert(leaf)
}

func (t *Tree) Delete(key []byte) error {
	return t.DeleteRaw(sum(key))
}

func (t *Tree) DeleteRaw(key [size]byte) error {
	if t.root == nil {
		return nil
	}
	_, _, err := t.root.Delete(key)
	return err
}

func (t *Tree) Hash() []byte {
	if t.root == nil {
		return zerosHash[:]
	}
	return t.root.Hash()
}

// Commit persists tree on disk and removes from memory.
func (t *Tree) Commit() error {
	if t.root == nil {
		return nil
	}
	t.root.Allocate()
	err := t.root.Commit()
	if err != nil {
		return err
	}
	t.version++
	buf := make([]byte, versionSize)
	marshalVersionTo(t.version, t.root, buf)
	n, err := t.store.WriteVersion(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return errors.New("incomplete version write")
	}
	err = t.store.Commit()
	if err != nil {
		return err
	}
	t.root = t.root.copy()
	return nil
}

func (t *Tree) LoadLatest() error {
	buf := make([]byte, versionSize)
	n, err := t.store.ReadLastVersion(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return errors.New("incomplete version read")
	}
	version, root, err := unmarshalVersion(t.store, buf)
	if err != nil {
		return err
	}
	t.version, t.root = version, root
	return nil
}

func (t *Tree) LoadVersion(version uint64) error {
	if version == 0 {
		return nil
	}
	buf := make([]byte, versionSize)
	n, err := t.store.ReadVersion(version, buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return errors.New("incomplete version read")
	}
	version, root, err := unmarshalVersion(t.store, buf)
	if err != nil {
		return err
	}
	t.version, t.root = version, root
	return nil
}

// Flush flushes tree to store buffers, potentially will be written to disk, but without fsync.
func (t *Tree) Flush() error {
	if t.root == nil {
		return nil
	}
	t.root.Allocate()
	err := t.root.Commit()
	if err != nil {
		return err
	}
	err = t.store.Flush()
	if err != nil {
		return err
	}
	t.root = t.root.copy()
	return nil
}

func (t *Tree) GenerateProof(key []byte, proof *Proof) error {
	return t.GenerateProofRaw(sum(key), proof)
}

func (t *Tree) GenerateProofRaw(key [size]byte, proof *Proof) error {
	if t.root == nil {
		return nil
	}
	return t.root.Prove(key, proof)

}

func (t *Tree) Snapshot() Snapshot {
	if t.root == nil {
		return nil
	}
	return &Tree{
		root:    t.root.copy(),
		store:   t.store,
		version: t.version,
	}
}

func (t *Tree) VersionSnapshot(version uint64) (Snapshot, error) {
	tree := &Tree{store: t.store}
	if err := tree.LoadVersion(version); err != nil {
		return nil, err
	}
	return tree, nil
}

type FlushTree struct {
	*Tree

	size, current int
}

func NewFlushTree(store *store.FileStore, size int) *FlushTree {
	return &FlushTree{Tree: NewTree(store), size: size}
}

func NewFlushTreeFromTree(tree *Tree, size int) *FlushTree {
	return &FlushTree{Tree: tree, size: size}
}

func (ft *FlushTree) Put(key, value []byte) error {
	err := ft.Tree.Put(key, value)
	if err != nil {
		return err
	}
	ft.current++
	if ft.current == ft.size {
		return ft.Flush()
	}
	return nil
}

func (ft *FlushTree) PutRaw(key [size]byte, preimage, value []byte) error {
	err := ft.Tree.PutRaw(key, preimage, value)
	if err != nil {
		return err
	}
	ft.current++
	if ft.current == ft.size {
		return ft.Flush()
	}
	return nil
}

func (ft *FlushTree) Flush() error {
	err := ft.Tree.Flush()
	if err != nil {
		return err
	}
	ft.current = 0
	return nil
}

func (ft *FlushTree) Commit() error {
	err := ft.Tree.Commit()
	if err != nil {
		return err
	}
	ft.current = 0
	return nil
}
