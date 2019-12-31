package urkeltrie

import (
	"errors"
	"fmt"
	"hash"
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

func newInner(store *FileStore, bit int) *inner {
	return &inner{
		store: store,
		bit:   bit,
		dirty: true,
		hash:  make([]byte, 0, size),
	}
}

func createInner(store *FileStore, bit int, idx, pos uint64, hash []byte) *inner {
	return &inner{
		bit:   bit,
		store: store,
		pos:   pos,
		idx:   idx,
		hash:  hash,
	}
}

type inner struct {
	store         *FileStore
	dirty, synced bool

	bit  int
	hash []byte

	pos, idx uint64

	left, right node
}

func (in *inner) copy() *inner {
	hash := in.Hash()
	return createInner(in.store, in.bit, in.idx, in.pos, hash[:])
}

func (in *inner) Allocate() {
	if in.dirty {
		in.idx, in.pos = in.store.TreeOffsetFor(in.Size())
		if in.left != nil {
			in.left.Allocate()
		}
		if in.right != nil {
			in.right.Allocate()
		}
	}
}

func (in *inner) Pos() uint64 {
	return in.pos
}

func (in *inner) Idx() uint64 {
	return in.idx
}

func (in *inner) Get(key [size]byte) ([]byte, error) {
	if err := in.sync(); err != nil {
		return nil, err
	}
	if bitSet(key, in.bit) {
		if in.right == nil {
			return nil, fmt.Errorf("right dead end at %d. key %x is not found", in.bit, key)
		}
		return in.right.Get(key)
	}
	if in.left == nil {
		return nil, fmt.Errorf("left dead end at %d. key %x is not found", in.bit, key)
	}
	return in.left.Get(key)
}

func (in *inner) sync() error {
	if !in.synced && !in.dirty {
		// sync the state from disk
		buf := make([]byte, in.Size())
		n, err := in.store.ReadTreeAt(in.idx, in.pos, buf)
		if err != nil {
			return fmt.Errorf("failed inner tree read at %d:%d. error %w", in.idx, in.pos, err)
		}
		if n != in.Size() {
			return fmt.Errorf("partial read for inner node: %d != %d", n, in.Size())
		}
		in.Unmarshal(buf)
		in.synced = true
	}
	return nil
}

func (in *inner) Insert(nodes ...*leaf) error {
	if err := in.sync(); err != nil {
		return err
	}
	in.dirty = true
	in.hash = in.hash[:0]
	for i := range nodes {
		n := nodes[i]
		if bitSet(n.key, in.bit) {
			if in.right == nil {
				in.right = n
				continue
			}
			switch tmp := in.right.(type) {
			case *inner:
				err := tmp.Insert(n)
				if err != nil {
					return err
				}
			case *leaf:
				if in.bit == lastBit {
					err := tmp.Put(n.key, n.value)
					if err != nil {
						return err
					}
					continue
				}
				if err := tmp.sync(); err != nil {
					return err
				}
				in.right = newInner(in.store, in.bit+1)
				err := in.right.(*inner).Insert(n, tmp)
				if err != nil {
					return err
				}
			}
		} else {
			if in.left == nil {
				in.left = n
				continue
			}
			switch tmp := in.left.(type) {
			case *inner:
				err := tmp.Insert(n)
				if err != nil {
					return err
				}
			case *leaf:
				if in.bit == lastBit {
					err := tmp.Put(n.key, n.value)
					if err != nil {
						return err
					}
					continue
				}
				if err := tmp.sync(); err != nil {
					return err
				}
				in.left = newInner(in.store, in.bit+1)
				err := in.left.(*inner).Insert(n, tmp)
				if err != nil {
					return err
				}
			}
		}
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

func (in *inner) Prove(key [32]byte, proof *Proof) error {
	if err := in.sync(); err != nil {
		return err
	}
	if bitSet(key, in.bit) {
		proof.AppendLeft(in.lhash())
		return in.right.Prove(key, proof)
	}
	proof.AppendRight(in.rhash())
	return in.left.Prove(key, proof)
}

func (in *inner) Commit() error {
	if !in.dirty {
		return nil
	}
	buf := innerPool.Get().([]byte)
	in.MarshalTo(buf)
	n, err := in.store.WriteTree(buf)
	for i := range buf {
		buf[i] = 0
	}
	innerPool.Put(buf)
	if err != nil {
		return err
	}
	if n != in.Size() {
		return errors.New("partial tree write")
	}
	in.dirty = false
	if in.left != nil {
		err = in.left.Commit()
		if err != nil {
			return err
		}
	}
	if in.right != nil {
		err = in.right.Commit()
		if err != nil {
			return err
		}
	}
	return nil
}

func (in *inner) Hash() []byte {
	if in.hash != nil && len(in.hash) > 0 {
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
		leftIdx   uint64
		leftPos   uint64
		leftHash  = zerosHash[:]
		rightIdx  uint64
		rightPos  uint64
		rightHash = zerosHash[:]
	)
	if in.left != nil {
		leftIdx = in.left.Idx()
		leftPos = in.left.Pos()
		leftHash = in.left.Hash()
	}
	if in.right != nil {
		rightIdx = in.right.Idx()
		rightPos = in.right.Pos()
		rightHash = in.right.Hash()
	}
	order.PutUint64(buf[2:], leftIdx)
	order.PutUint64(buf[10:], leftPos)
	order.PutUint64(buf[18:], rightIdx)
	order.PutUint64(buf[26:], rightPos)
	copy(buf[34:], leftHash[:])
	copy(buf[66:], rightHash[:])
}

func (in *inner) Unmarshal(buf []byte) {
	_ = buf[in.Size()-1]
	ltype := buf[0]
	rtype := buf[1]
	leftIdx := order.Uint64(buf[2:])
	leftPos := order.Uint64(buf[10:])
	rightIdx := order.Uint64(buf[18:])
	rightPos := order.Uint64(buf[26:])
	if ltype != nullNode {
		leftHash := make([]byte, 32)
		copy(leftHash, buf[34:])
		if ltype == innerNode {
			in.left = createInner(in.store, in.bit+1, leftIdx, leftPos, leftHash)
		} else if ltype == leafNode {
			in.left = createLeaf(in.store, leftIdx, leftPos)
		}
	}
	if rtype != nullNode {
		rightHash := make([]byte, 32)
		copy(rightHash, buf[66:])
		if rtype == innerNode {
			in.right = createInner(in.store, in.bit+1, rightIdx, rightPos, rightHash)
		} else if rtype == leafNode {
			in.right = createLeaf(in.store, rightIdx, rightPos)
		}
	}
}
