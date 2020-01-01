package urkeltrie

import (
	"encoding/binary"
	"errors"
	"hash"
	"sync"

	"github.com/dshulyak/urkeltrie/store"
	"golang.org/x/crypto/blake2s"
)

const (
	size    = 32
	lastBit = size*8 - 1

	leafDomain  = 0x01
	innerDomain = 0x02

	// 4 bytes for position is enough
	// 32 bits for position in a file >3gb
	// 2 bytes for file index is enough
	// 16 bits - 65k files, ~200 tb db size
	leafSize    = 32 + 8 + 8 + 8       // key (hash), value idx, value pos, value length
	innerSize   = 2 + 2*8 + 2*8 + 2*32 // node type *2, leaf idx x 2, leaf pos x 2, leaf hashses x 2
	versionSize = 24 + 32              // version, idx, pos, hash
)

var (
	zeros, zerosHash [size]byte
	order            = binary.BigEndian

	digestPool = sync.Pool{New: func() interface{} { return hasher() }}
	innerPool  = sync.Pool{New: func() interface{} { return make([]byte, innerSize) }}
)

func init() {
	h := hasher()
	h.Write([]byte{leafDomain})
	h.Write(zeros[:])
	tmp := zerosHash[:0]
	h.Sum(tmp)
}

func hasher() hash.Hash {
	h, _ := blake2s.New256(nil)
	return h
}

func sum(key []byte) (rst [size]byte) {
	h := hasher()
	h.Write(key)
	h.Sum(rst[:0])
	return
}

type node interface {
	Get([size]byte) ([]byte, error)
	Hash() []byte
	Allocate()
	Position() (uint64, uint64)
	Commit() error
	Prove([size]byte, *Proof) error
	Sync() error
}

func NewTree(store *store.FileStore) *Tree {
	return &Tree{store: store}
}

type Tree struct {
	store *store.FileStore

	mu      sync.Mutex
	version uint64
	root    *inner
}

func (t *Tree) Get(key []byte) ([]byte, error) {
	return t.GetRaw(sum(key))
}

func (t *Tree) GetRaw(key [size]byte) ([]byte, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.root == nil {
		return nil, errors.New("not found")
	}
	return t.root.Get(key)
}

func (t *Tree) Put(key, value []byte) error {
	return t.PutRaw(sum(key), value)
}

func (t *Tree) PutRaw(key [size]byte, value []byte) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.root == nil {
		t.root = newInner(t.store, 0)
	}
	leaf := newLeaf(t.store, key, value)
	return t.root.Insert(leaf)
}

func (t *Tree) Hash() []byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.root == nil {
		return zerosHash[:]
	}
	return t.root.Hash()
}

// Commit persists tree on disk and removes from memory.
func (t *Tree) Commit() error {
	t.mu.Lock()
	defer t.mu.Unlock()
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
	t.mu.Lock()
	defer t.mu.Unlock()
	buf := make([]byte, versionSize)
	n, err := t.store.ReadLastVersion(buf)
	if err != nil {
		return err
	}
	if n != len(buf) {
		return errors.New("incomplete version read")
	}
	t.version, t.root = unmarshalVersion(t.store, buf)
	return nil
}

func (t *Tree) LoadVersion(version uint64) error {
	t.mu.Lock()
	defer t.mu.Unlock()
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
	t.version, t.root = unmarshalVersion(t.store, buf)
	return nil
}

// Flush flushes tree to store buffers, potentially will be written to disk, but without fsync.
func (t *Tree) Flush() error {
	t.mu.Lock()
	defer t.mu.Unlock()
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
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.root == nil {
		return nil
	}
	return t.root.Prove(sum(key), proof)
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

func (ft *FlushTree) PutRaw(key [size]byte, value []byte) error {
	err := ft.Tree.PutRaw(key, value)
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

func marshalVersionTo(version uint64, node *inner, buf []byte) {
	order.PutUint64(buf, version)
	idx, pos := node.Position()
	order.PutUint64(buf[8:], idx)
	order.PutUint64(buf[16:], pos)
	copy(buf[24:], node.Hash())
}

func unmarshalVersion(store *store.FileStore, buf []byte) (uint64, *inner) {
	var (
		version, idx, pos uint64
		hash              = make([]byte, 32)
	)
	version = order.Uint64(buf)
	idx = order.Uint64(buf[8:])
	pos = order.Uint64(buf[16:])
	copy(hash, buf[24:])
	return version, createInner(store, 0, idx, pos, hash)
}
