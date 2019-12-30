package urkeltrie

import (
	"encoding/binary"
	"errors"
	"hash"
	"sync"

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
	leafSize = 32 + 8 + 8 + 8 // key, value idx, value pos, value length
	// bit position is not required, can be passed from parent
	innerSize = 2 + 2*8 + 2*8 + 2*32 // bit position, leaf idx x 2, leaf pos x 2, leaf hashses x 2
)

var (
	zeros, zerosHash [size]byte
	order            = binary.BigEndian

	digestPool = sync.Pool{New: func() interface{} { return hasher() }}
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
	Put([size]byte, []byte) error
	Hash() [size]byte
	// Pos and Idx of the node in the file store.
	Allocate()
	Pos() uint64
	Idx() uint64
	Commit() error
	Prove([size]byte, *Proof) error
}

func NewTree(store *FileStore) *Tree {
	return &Tree{store: store}
}

type Tree struct {
	store *FileStore

	mu   sync.Mutex
	root *inner
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
	return t.root.Put(key, value)
}

func (t *Tree) Hash() [size]byte {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.root == nil {
		return zerosHash
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
	err = t.store.Commit()
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
