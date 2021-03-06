package urkeltrie

import (
	"errors"
	"fmt"
	"hash"

	"github.com/dshulyak/urkeltrie/store"
)

var (
	ErrNotFound = errors.New("leaf not found")
	ErrCRC      = errors.New("entry corrupted")
)

const (
	nullNode byte = iota
	leafNode
	innerNode
)

func nodeType(n node) byte {
	if n != nil {
		switch n.(type) {
		case *inner:
			return innerNode
		case *leaf:
			return leafNode
		default:
			panic("unknown type")
		}
	}
	return nullNode
}

func newInner(bit uint8) *inner {
	return &inner{
		bit:   bit,
		dirty: true,
		hash:  make([]byte, 0, size),
	}
}

func createInner(bit uint8, idx, pos uint32, hash []byte) *inner {
	return &inner{
		bit:  bit,
		pos:  pos,
		idx:  idx,
		hash: hash,
	}
}

type inner struct {
	dirty, synced bool

	bit  uint8
	hash []byte

	pos, idx uint32

	left, right node
}

func (in *inner) String() string {
	return fmt.Sprintf("Inner<%d,%d:%d>", in.bit, in.idx, in.pos)
}

func (in *inner) copy() *inner {
	return createInner(in.bit, in.idx, in.pos, in.Hash())
}

func (in *inner) Allocate(store *store.FileStore) {
	if in.dirty {
		in.idx, in.pos = store.TreeOffsetFor(in.Size())
		if in.left != nil {
			in.left.Allocate(store)
		}
		if in.right != nil {
			in.right.Allocate(store)
		}
	}
}

func (in *inner) Position() (uint32, uint32) {
	return in.idx, in.pos
}

func (in *inner) iterateChild(store *store.FileStore, child node, reverse bool, iterf IterateFunc) (bool, error) {
	if child == nil {
		return false, nil
	}
	switch n := child.(type) {
	case *inner:
		return n.iterate(store, reverse, iterf)
	case *leaf:
		entry, err := n.makeEntry(store)
		if err != nil {
			return false, err
		}
		return iterf(entry), nil
	}
	return false, nil
}

func (in *inner) iterate(store *store.FileStore, reverse bool, iterf IterateFunc) (bool, error) {
	if err := in.sync(store); err != nil {
		return false, err
	}
	defer in.reset()
	childs := []node{in.left, in.right}
	if reverse {
		childs[0], childs[1] = childs[1], childs[0]
	}
	for _, child := range childs {
		stop, err := in.iterateChild(store, child, reverse, iterf)
		if err != nil {
			return false, err
		}
		if stop {
			return true, nil
		}
	}
	return false, nil
}

func (in *inner) Get(store *store.FileStore, key [size]byte) ([]byte, error) {
	if err := in.sync(store); err != nil {
		return nil, err
	}
	defer in.reset()
	if bitSet(key, in.bit) {
		if in.right == nil {
			return nil, fmt.Errorf("%w: right dead end at %d. key %x", ErrNotFound, in.bit, key)
		}
		return in.right.Get(store, key)
	}
	if in.left == nil {
		return nil, fmt.Errorf("%w: left dead end at %d. key %x", ErrNotFound, in.bit, key)
	}
	return in.left.Get(store, key)
}

func (in *inner) Sync(store *store.FileStore) error {
	return in.sync(store)
}

func (in *inner) sync(store *store.FileStore) error {
	if !in.synced && !in.dirty {
		// sync the state from disk
		buf := make([]byte, in.Size())
		n, err := store.ReadTreeAt(in.idx, in.pos, buf)
		if err != nil {
			return fmt.Errorf("failed inner tree read at %d:%d. error %w", in.idx, in.pos, err)
		}
		if n != in.Size() {
			return fmt.Errorf("partial read for inner node: %d != %d", n, in.Size())
		}
		if err := in.Unmarshal(buf); err != nil {
			return err
		}
		in.synced = true
	}
	return nil
}

func (in *inner) reset() {
	// TODO this is good place to use freelist for inner nodes
	// load on gc from instantiating them is noticeable.
	if !in.childsDirty() && !in.isDirty() {
		in.left = nil
		in.synced = false
		in.right = nil
		in.synced = false
	}
}

func (in *inner) isDirty() bool {
	return in.dirty
}

func (in *inner) leftDirty() bool {
	if in.left == nil {
		return false
	}
	return in.left.isDirty()
}

func (in *inner) rightDirty() bool {
	if in.right == nil {
		return false
	}
	return in.right.isDirty()
}

func (in *inner) childsDirty() bool {
	return in.leftDirty() && in.rightDirty()
}

func (in *inner) empty() bool {
	return in.left == nil && in.right == nil
}

func (in *inner) Delete(store *store.FileStore, key [size]byte) (bool, bool, error) {
	if err := in.sync(store); err != nil {
		return false, false, err
	}
	if bitSet(key, in.bit) {
		if in.right == nil {
			return false, false, nil
		}
		empty, changed, err := in.right.Delete(store, key)
		if err != nil {
			return false, false, err
		}
		if changed {
			in.dirty = true
			in.hash = in.hash[:0]
		}
		if empty {
			in.right = nil
			return in.empty(), changed, nil
		}
		return false, changed, nil
	}
	if in.left == nil {
		return false, false, nil
	}
	empty, changed, err := in.left.Delete(store, key)
	if err != nil {
		return false, false, err
	}
	if changed {
		in.dirty = true
		in.hash = in.hash[:0]
	}
	if empty {
		in.left = nil
		return in.empty(), changed, nil
	}
	return false, changed, nil
}

func (in *inner) Insert(store *store.FileStore, nodes ...*leaf) error {
	if err := in.sync(store); err != nil {
		return err
	}
	in.dirty = true
	in.hash = in.hash[:0]
	for i := range nodes {
		n := nodes[i]
		if err := in.insert(store, n); err != nil {
			return err
		}
	}
	return nil
}

func (in *inner) insert(store *store.FileStore, n *leaf) error {
	if bitSet(n.key, in.bit) {
		if in.right == nil {
			in.right = n
			return nil
		}
		switch tmp := in.right.(type) {
		case *inner:
			return tmp.Insert(store, n)
		case *leaf:
			if in.bit == lastBit {
				return tmp.Put(store, n.key, n.value)
			}
			if err := tmp.Sync(store); err != nil {
				return err
			}
			in.right = newInner(in.bit + 1)
			return in.right.(*inner).Insert(store, tmp, n)

		}
		return nil
	}
	if in.left == nil {
		in.left = n
		return nil
	}
	switch tmp := in.left.(type) {
	case *inner:
		return tmp.Insert(store, n)
	case *leaf:
		if in.bit == lastBit {
			return tmp.Put(store, n.key, n.value)
		}
		if err := tmp.Sync(store); err != nil {
			return err
		}
		in.left = newInner(in.bit + 1)
		return in.left.(*inner).Insert(store, tmp, n)
	}
	return nil
}

func (in *inner) lhash() []byte {
	if in.left != nil {
		return in.left.Hash()
	}
	return zerosHash[:]
}

func (in *inner) rhash() []byte {
	if in.right != nil {
		return in.right.Hash()
	}
	return zerosHash[:]
}

func (in *inner) Prove(store *store.FileStore, key [32]byte, proof *Proof) error {
	if err := in.sync(store); err != nil {
		return err
	}
	defer in.reset()
	if bitSet(key, in.bit) {
		proof.addTrace(in.lhash())
		if in.right != nil {
			return in.right.Prove(store, key, proof)
		}
		proof.addDeadend()
		return nil
	}
	proof.addTrace(in.rhash())
	if in.left != nil {
		return in.left.Prove(store, key, proof)
	}
	proof.addDeadend()
	return nil
}

func (in *inner) Commit(store *store.FileStore) error {
	if !in.dirty {
		return nil
	}
	buf := innerPool.Get().([]byte)
	in.MarshalTo(buf)
	n, err := store.WriteTree(buf)
	for i := range buf {
		buf[i] = 0
	}
	if err != nil {
		return err
	}
	innerPool.Put(buf)
	if n != in.Size() {
		return errors.New("partial tree write")
	}
	in.dirty = false
	if in.left != nil {
		err = in.left.Commit(store)
		if err != nil {
			return err
		}
	}
	if in.right != nil {
		err = in.right.Commit(store)
		if err != nil {
			return err
		}
	}
	// eagerly prehash nodes starting from the leafs of the inserted branch
	_ = in.Hash()
	return nil
}

func (in *inner) Hash() []byte {
	if len(in.hash) > 0 {
		return in.hash
	}
	h := digestPool.Get().(hash.Hash)
	h.Write([]byte{innerDomain})
	if in.left != nil && in.right != nil {
		c := results.Get().(chan []byte)
		go func() {
			c <- in.rhash()
		}()
		h.Write(in.lhash())
		h.Write(<-c)
		results.Put(c)
	} else {
		h.Write(in.lhash())
		h.Write(in.rhash())
	}
	in.hash = h.Sum(in.hash)
	h.Reset()
	digestPool.Put(h)
	return in.hash
}

func (in *inner) Size() int {
	return innerSize
}

func (in *inner) Marshal() []byte {
	buf := make([]byte, in.Size())
	in.MarshalTo(buf)
	return buf
}

func (in *inner) MarshalTo(buf []byte) {
	_ = buf[in.Size()-1]
	buf[0] = nodeType(in.left)
	buf[1] = nodeType(in.right)
	var (
		leftIdx   uint32
		leftPos   uint32
		leftHash  = zerosHash[:]
		rightIdx  uint32
		rightPos  uint32
		rightHash = zerosHash[:]
	)
	if in.left != nil {
		leftIdx, leftPos = in.left.Position()
		leftHash = in.left.Hash()
	}
	if in.right != nil {
		rightIdx, rightPos = in.right.Position()
		rightHash = in.right.Hash()
	}
	order.PutUint32(buf[2:], leftIdx)
	order.PutUint32(buf[6:], leftPos)
	order.PutUint32(buf[10:], rightIdx)
	order.PutUint32(buf[14:], rightPos)
	copy(buf[18:], leftHash[:])
	copy(buf[50:], rightHash[:])
	putCrcSum32(buf[82:86], buf[:82])
}

func (in *inner) Unmarshal(buf []byte) error {
	_ = buf[in.Size()-1]
	// crc unmarshals in big endian as well
	if crcSum32(buf[:82]) != order.Uint32(buf[82:]) {
		return fmt.Errorf("%w: inner node at height %d", ErrCRC, in.bit)
	}
	ltype := buf[0]
	rtype := buf[1]
	leftIdx := order.Uint32(buf[2:])
	leftPos := order.Uint32(buf[6:])
	rightIdx := order.Uint32(buf[10:])
	rightPos := order.Uint32(buf[14:])
	if ltype != nullNode {
		leftHash := make([]byte, 32)
		copy(leftHash, buf[18:])
		if ltype == innerNode {
			in.left = createInner(in.bit+1, leftIdx, leftPos, leftHash)
		} else if ltype == leafNode {
			in.left = createLeaf(leftIdx, leftPos, leftHash)
		}
	}
	if rtype != nullNode {
		rightHash := make([]byte, 32)
		copy(rightHash, buf[50:])
		if rtype == innerNode {
			in.right = createInner(in.bit+1, rightIdx, rightPos, rightHash)
		} else if rtype == leafNode {
			in.right = createLeaf(rightIdx, rightPos, rightHash)
		}
	}
	return nil
}
