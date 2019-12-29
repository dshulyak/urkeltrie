package urkeltrie

import (
	"errors"
)

func newInner(store *FileStore, bit int) *inner {
	return &inner{
		store:     store,
		bit:       bit,
		dirty:     true,
		leftHash:  zerosHash,
		rightHash: zerosHash,
	}
}

func createInner(store *FileStore, pos, idx uint64) *inner {
	return &inner{
		store:     store,
		pos:       pos,
		idx:       idx,
		leftHash:  zerosHash,
		rightHash: zerosHash,
	}
}

type inner struct {
	store         *FileStore
	dirty, synced bool

	bit  int
	hash [size]byte

	pos, idx            uint64
	leftIdx, leftPos    uint64
	rightIdx, rightPos  uint64
	leftHash, rightHash [size]byte

	left, right node
}

func (in *inner) presync() error {
	if in.dirty {
		in.pos, in.idx = in.store.TreeOffsetFor(in.Size())
	} else {
		buf := make([]byte, in.Size())
		_, err := in.store.ReadValueAt(in.idx, in.pos, buf)
		if err != nil {
			return err
		}
		in.Unmarshal(buf)
	}
	return nil
}

func (in *inner) Pos() uint64 {
	return in.pos
}

func (in *inner) Idx() uint64 {
	return in.idx
}

func (in *inner) Get(key [size]byte) ([]byte, error) {
	if !in.synced {
		err := in.presync()
		if err != nil {
			return nil, err
		}
		in.synced = true
	}
	if bitSet(key, in.bit) {
		if in.bit == lastBit {
			if in.rightIdx == 0 && in.rightPos == 0 {
				return nil, errors.New("not found")
			}
			if in.right == nil {
				in.right = createLeaf(in.store, in.rightIdx, in.rightPos)
			}
		} else if in.right == nil {
			if in.rightHash == zerosHash {
				return nil, errors.New("not found")
			}
			in.right = createInner(in.store, in.rightIdx, in.rightPos)
		}
		return in.right.Get(key)
	}
	if in.bit == lastBit {
		if in.leftIdx == 0 && in.leftPos == 0 {
			return nil, errors.New("not found")
		}
		if in.left == nil {
			in.left = createLeaf(in.store, in.leftIdx, in.leftPos)
		}
	} else if in.left == nil {
		if in.leftHash == zerosHash {
			return nil, errors.New("not found")
		}
		in.left = createInner(in.store, in.leftIdx, in.leftPos)
	}
	return in.left.Get(key)
}

func (in *inner) Put(key [size]byte, value []byte) (err error) {
	if !in.synced {
		// request new position for new insertions
		// or just sync the state from disk
		err = in.presync()
		if err != nil {
			return err
		}
		in.synced = true
	}
	if !in.dirty {
		// request new position in the tree file for new branch
		// we don't request new position for uncommited branches
		in.dirty = true
		in.hash = zeros
		err = in.presync()
		if err != nil {
			return
		}
	}
	if bitSet(key, in.bit) {
		if in.bit == lastBit {
			if in.rightHash == zerosHash {
				in.right = newLeaf(in.store, key, value)
			} else {
				in.right = createLeaf(in.store, in.rightIdx, in.rightPos)
			}
		} else if in.right == nil {
			if in.rightIdx == 0 && in.rightPos == 0 {
				in.right = newInner(in.store, in.bit+1)
			} else {
				in.right = createInner(in.store, in.rightIdx, in.rightPos)
			}
		}
		err = in.right.Put(key, value)
		if err != nil {
			return
		}
		in.rightIdx, in.rightPos = in.right.Idx(), in.right.Pos()
		return
	}
	if in.bit == lastBit {
		if in.leftIdx == 0 && in.leftPos == 0 {
			in.left = newLeaf(in.store, key, value)
		} else {
			in.left = createLeaf(in.store, in.leftIdx, in.leftPos)
		}
	} else if in.left == nil {
		if in.leftHash == zerosHash {
			in.left = newInner(in.store, in.bit+1)
		} else {
			in.left = createInner(in.store, in.leftIdx, in.leftPos)
		}
	}
	err = in.left.Put(key, value)
	in.leftIdx, in.leftPos = in.left.Idx(), in.left.Pos()
	return
}

func (in *inner) Commit() error {
	if !in.dirty {
		return nil
	}
	n, err := in.store.WriteValue(in.Marshal())
	if err != nil {
		return err
	}
	if n != in.Size() {
		return errors.New("partial tree write")
	}
	in.dirty = false
	err = in.left.Commit()
	if err != nil {
		return err
	}
	err = in.right.Commit()
	if err != nil {
		return err
	}
	return nil
}

func (in *inner) Hash() (rst [size]byte) {
	if in.hash != zeros {
		return in.hash
	}
	h := hasher()
	h.Write([]byte{innerDomain})
	if in.left != nil {
		tmp := in.left.Hash()
		h.Write(tmp[:])
	} else {
		h.Write(zerosHash[:])
	}
	if in.right != nil {
		tmp := in.right.Hash()
		h.Write(tmp[:])
	} else {
		h.Write(zerosHash[:])
	}
	h.Sum(rst[:0])
	in.hash = rst
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
	idx := 2
	order.PutUint64(buf[idx:], in.leftIdx)
	idx += 8
	order.PutUint64(buf[idx:], in.leftPos)
	idx += 8
	order.PutUint64(buf[idx:], in.rightIdx)
	idx += 8
	order.PutUint64(buf[idx:], in.rightPos)
	idx += 8
	hash := in.leftHash
	if in.left != nil {
		hash = in.left.Hash()
	}
	copy(buf[idx:], hash[:])
	idx += 32
	hash = in.rightHash
	if in.right != nil {
		hash = in.right.Hash()
	}
	copy(buf[idx:], hash[:])
}

func (in *inner) Unmarshal(buf []byte) {
	_ = buf[in.Size()-1]
	in.bit = int(order.Uint16(buf))
	in.leftIdx = order.Uint64(buf[2:])
	in.leftPos = order.Uint64(buf[10:])
	in.rightIdx = order.Uint64(buf[18:])
	in.rightPos = order.Uint64(buf[26:])
	copy(in.leftHash[:], buf[34:])
	copy(in.rightHash[:], buf[66:])
}
