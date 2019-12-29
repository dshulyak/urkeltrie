package urkeltrie

import (
	"bytes"
	"errors"
	"fmt"
)

func newInner(store *FileStore, bit int) *inner {
	return &inner{
		store: store,
		bit:   bit,
		dirty: true,
	}
}

func createInner(store *FileStore, idx, pos uint64) *inner {
	return &inner{
		store: store,
		pos:   pos,
		idx:   idx,
	}
}

type inner struct {
	store         *FileStore
	dirty, synced bool

	bit  int
	hash []byte

	// this space is wasted
	// all we need to store per node is pos and idx
	pos, idx            uint64
	leftIdx, leftPos    uint64
	rightIdx, rightPos  uint64
	leftHash, rightHash []byte

	left, right node
}

func (in *inner) copy() *inner {
	return createInner(in.store, in.idx, in.pos)
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
			in.leftIdx, in.leftPos = in.left.Idx(), in.left.Pos()
		}
		if in.right != nil {
			in.right.Allocate()
			in.rightIdx, in.rightPos = in.right.Idx(), in.right.Pos()
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
		if in.bit == lastBit {
			if in.right == nil && in.rightHash == nil {
				return nil, fmt.Errorf("reached last bit. key %x is not found", key)
			} else if in.right == nil {
				in.right = createLeaf(in.store, in.rightIdx, in.rightPos)
			}
		} else if in.right == nil {
			if in.rightHash == nil {
				return nil, fmt.Errorf("right dead end at %d. key %x is not found", in.bit, key)
			}
			in.right = createInner(in.store, in.rightIdx, in.rightPos)
		}
		return in.right.Get(key)
	}
	if in.bit == lastBit {
		if in.left == nil && in.leftHash == nil {
			return nil, fmt.Errorf("reached last bit. key %x is not found", key)
		} else if in.left == nil {
			in.left = createLeaf(in.store, in.leftIdx, in.leftPos)
		}
	} else if in.left == nil {
		if in.leftHash == nil {
			return nil, fmt.Errorf("left dead end at %d. key %x is not found", in.bit, key)
		}
		in.left = createInner(in.store, in.leftIdx, in.leftPos)
	}
	return in.left.Get(key)
}

func (in *inner) sync() error {
	if !in.synced {
		// request new position for new insertions
		// or just sync the state from disk
		err := in.presync()
		if err != nil {
			return err
		}
		in.synced = true
	}
	return nil
}

func (in *inner) Put(key [size]byte, value []byte) (err error) {
	if err := in.sync(); err != nil {
		return err
	}
	if bitSet(key, in.bit) {
		if in.bit == lastBit {
			if in.rightHash == nil {
				in.right = newLeaf(in.store, key, value)
			} else if in.right == nil {
				in.right = createLeaf(in.store, in.rightIdx, in.rightPos)
			}
		} else if in.right == nil {
			if in.rightHash == nil {
				in.right = newInner(in.store, in.bit+1)
			} else {
				in.right = createInner(in.store, in.rightIdx, in.rightPos)
			}
		}
		err = in.right.Put(key, value)
		if err != nil {
			return
		}
		return
	}
	if in.bit == lastBit {
		if in.leftHash == nil {
			in.left = newLeaf(in.store, key, value)
		} else {
			in.left = createLeaf(in.store, in.leftIdx, in.leftPos)
		}
	} else if in.left == nil {
		if in.leftHash == nil {
			in.left = newInner(in.store, in.bit+1)
		} else {
			in.left = createInner(in.store, in.leftIdx, in.leftPos)
		}
	}
	err = in.left.Put(key, value)
	return
}

func (in *inner) lhash() [size]byte {
	hash := [size]byte{}
	if in.left != nil {
		hash = in.left.Hash()
	} else if in.leftHash != nil {
		copy(hash[:], in.leftHash)
	} else {
		hash = zerosHash
	}
	return hash
}

func (in *inner) rhash() [size]byte {
	hash := [size]byte{}
	if in.right != nil {
		hash = in.right.Hash()
	} else if in.rightHash != nil {
		copy(hash[:], in.rightHash)
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
		if in.right == nil && in.rightHash != nil {
			in.right = createInner(in.store, in.rightIdx, in.rightPos)
		}
		return in.right.Prove(key, proof)
	}
	proof.AppendRight(in.rhash())
	if in.bit == lastBit {
		return nil
	}
	if in.left == nil && in.leftHash != nil {
		in.left = createInner(in.store, in.leftIdx, in.leftPos)
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
	h := hasher()
	h.Write([]byte{innerDomain})
	lhash := in.lhash()
	h.Write(lhash[:])
	rhash := in.rhash()
	h.Write(rhash[:])
	h.Sum(rst[:0])
	in.hash = rst[:]
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
	order.PutUint64(buf[2:], in.leftIdx)
	order.PutUint64(buf[10:], in.leftPos)
	order.PutUint64(buf[18:], in.rightIdx)
	order.PutUint64(buf[26:], in.rightPos)
	hash := in.lhash()
	copy(buf[34:], hash[:])
	hash = in.rhash()
	copy(buf[66:], hash[:])
}

func (in *inner) Unmarshal(buf []byte) {
	_ = buf[in.Size()-1]
	in.bit = int(order.Uint16(buf))
	in.leftIdx = order.Uint64(buf[2:])
	in.leftPos = order.Uint64(buf[10:])
	in.rightIdx = order.Uint64(buf[18:])
	in.rightPos = order.Uint64(buf[26:])
	if bytes.Compare(buf[34:66], zerosHash[:]) != 0 {
		in.leftHash = make([]byte, 32)
		copy(in.leftHash[:], buf[34:])
	}
	if bytes.Compare(buf[66:], zerosHash[:]) != 0 {
		in.rightHash = make([]byte, 32)
		copy(in.rightHash[:], buf[66:])
	}
}
