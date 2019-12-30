package urkeltrie

import (
	"bytes"
	"errors"
	"fmt"
	"hash"
)

func newInner(store *FileStore, bit int) *inner {
	return &inner{
		store: store,
		bit:   bit,
		dirty: true,
	}
}

func createInner(store *FileStore, idx, pos uint64, hash []byte) *inner {
	return &inner{
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
	return createInner(in.store, in.idx, in.pos, hash[:])
}

func (in *inner) presync() error {
	if !in.dirty {
		buf := make([]byte, in.Size())
		n, err := in.store.ReadTreeAt(in.idx, in.pos, buf)
		if err != nil {
			return fmt.Errorf("failed inner tree read at %d:%d. error %w", in.idx, in.pos, err)
		}
		if n != in.Size() {
			return fmt.Errorf("partial read for inner node: %d != %d", n, in.Size())
		}
		in.Unmarshal(buf)
	}
	return nil
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
		if in.bit == lastBit && in.right == nil {
			return nil, fmt.Errorf("reached last bit. key %x is not found", key)
		} else if in.right == nil {
			return nil, fmt.Errorf("right dead end at %d. key %x is not found", in.bit, key)
		}
		return in.right.Get(key)
	}
	if in.bit == lastBit && in.left == nil {
		return nil, fmt.Errorf("reached last bit. key %x is not found", key)
	} else if in.left == nil {
		return nil, fmt.Errorf("left dead end at %d. key %x is not found", in.bit, key)
	}
	return in.left.Get(key)
}

func (in *inner) sync() error {
	if !in.synced {
		// request new position for new insertions
		// or just sync the state from disk
		if !in.dirty {
			buf := make([]byte, in.Size())
			n, err := in.store.ReadTreeAt(in.idx, in.pos, buf)
			if err != nil {
				return fmt.Errorf("failed inner tree read at %d:%d. error %w", in.idx, in.pos, err)
			}
			if n != in.Size() {
				return fmt.Errorf("partial read for inner node: %d != %d", n, in.Size())
			}
			in.Unmarshal(buf)
		}
		in.synced = true
	}
	return nil
}

func (in *inner) Put(key [size]byte, value []byte) (err error) {
	if err := in.sync(); err != nil {
		return err
	}
	in.dirty = true
	in.hash = nil
	if bitSet(key, in.bit) {
		if in.bit == lastBit && in.right == nil {
			in.right = newLeaf(in.store, key, value)
		} else if in.right == nil {
			in.right = newInner(in.store, in.bit+1)
		}
		err = in.right.Put(key, value)
		if err != nil {
			return
		}
		return
	}
	if in.bit == lastBit && in.left == nil {
		in.left = newLeaf(in.store, key, value)
	} else if in.left == nil {
		in.left = newInner(in.store, in.bit+1)
	}
	err = in.left.Put(key, value)
	return
}

func (in *inner) lhash() [size]byte {
	hash := [size]byte{}
	if in.left != nil {
		hash = in.left.Hash()
	} else {
		hash = zerosHash
	}
	return hash
}

func (in *inner) rhash() [size]byte {
	hash := [size]byte{}
	if in.right != nil {
		hash = in.right.Hash()
	} else {
		hash = zerosHash
	}
	return hash
}

func (in *inner) Prove(key [32]byte, proof *Proof) error {
	if err := in.sync(); err != nil {
		return err
	}
	if bitSet(key, in.bit) {
		proof.AppendLeft(in.lhash())
		if in.bit == lastBit {
			// leaves required only for non-membership proves
			return nil
		}
		return in.right.Prove(key, proof)
	}
	proof.AppendRight(in.rhash())
	if in.bit == lastBit {
		return nil
	}
	return in.left.Prove(key, proof)
}

func (in *inner) Commit() error {
	if !in.dirty {
		return nil
	}
	n, err := in.store.WriteTree(in.Marshal())
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

func (in *inner) Hash() (rst [size]byte) {
	if in.hash != nil {
		copy(rst[:], in.hash)
		return rst
	}
	if err := in.sync(); err != nil {
		return
	}
	h := digestPool.Get().(hash.Hash)
	h.Write([]byte{innerDomain})
	lhash := in.lhash()
	h.Write(lhash[:])
	rhash := in.rhash()
	h.Write(rhash[:])
	h.Sum(rst[:0])
	in.hash = rst[:]
	h.Reset()
	digestPool.Put(h)
	return
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
	order.PutUint16(buf, uint16(in.bit))
	var (
		leftIdx   uint64
		leftPos   uint64
		leftHash  = zerosHash
		rightIdx  uint64
		rightPos  uint64
		rightHash = zerosHash
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
	in.bit = int(order.Uint16(buf))
	leftIdx := order.Uint64(buf[2:])
	leftPos := order.Uint64(buf[10:])
	rightIdx := order.Uint64(buf[18:])
	rightPos := order.Uint64(buf[26:])
	if bytes.Compare(buf[34:66], zerosHash[:]) != 0 {
		leftHash := make([]byte, 32)
		copy(leftHash, buf[34:])
		if in.bit != lastBit {
			in.left = createInner(in.store, leftIdx, leftPos, leftHash)
		} else {
			in.left = createLeaf(in.store, leftIdx, leftPos)
		}
	}
	if bytes.Compare(buf[66:], zerosHash[:]) != 0 {
		rightHash := make([]byte, 32)
		copy(rightHash, buf[66:])
		if in.bit != lastBit {
			in.right = createInner(in.store, rightIdx, rightPos, rightHash)
		} else {
			in.right = createLeaf(in.store, rightIdx, rightPos)
		}
	}
}
