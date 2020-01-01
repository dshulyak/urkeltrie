package urkeltrie

import (
	"bytes"
)

const (
	member byte = iota + 1
	collision
	deadend
)

func NewProof(hint int) *Proof {
	return &Proof{
		trace: make([][]byte, 0, hint),
	}
}

type Proof struct {
	ptype byte
	trace [][]byte
	value []byte

	// collision key and value
	ckey, cval []byte
}

func (p *Proof) Reset() {
	for i := range p.trace {
		p.trace[i] = nil
	}
	p.value = nil
	p.ckey = nil
	p.cval = nil
	p.ptype = 0
	p.trace = p.trace[:0]
}

func (p *Proof) addTrace(hash []byte) {
	p.trace = append(p.trace, hash)
}

func (p *Proof) addDeadend() {
	p.ptype = deadend
}

func (p *Proof) addValue(value []byte) {
	p.ptype = member
	p.value = value
}

func (p *Proof) addCollision(key, val []byte) {
	p.ptype = collision
	p.ckey = key
	p.cval = val
}

func (p *Proof) rootForLeaf(lkey [32]byte, leaf []byte) []byte {
	h := hasher()
	rst := make([]byte, 32)
	copy(rst, leaf)
	for i := len(p.trace) - 1; i >= 0; i-- {
		sibling := p.trace[i]
		h.Write([]byte{innerDomain})
		if bitSet(lkey, i) {
			h.Write(sibling)
			h.Write(rst)
		} else {
			h.Write(rst)
			h.Write(sibling)
		}
		rst = h.Sum(rst[:0])
		h.Reset()
	}
	return rst
}

func (p *Proof) VerifyMembership(root, key []byte) bool {
	return p.VerifyMembershipRaw(root, sum(key))
}

func (p *Proof) VerifyMembershipRaw(root []byte, key [size]byte) bool {
	return bytes.Compare(root, p.rootForLeaf(key, leafHash(key[:], p.value))) == 0
}

func (p *Proof) VerifyNonMembership(root, key []byte) bool {
	return p.VerifyNonMembershipRaw(root, sum(key))
}

func (p *Proof) VerifyNonMembershipRaw(root []byte, key [size]byte) bool {
	if p.ptype == collision {
		return bytes.Compare(root, p.rootForLeaf(key, leafHash(p.ckey, p.cval))) == 0
	}
	if p.ptype == deadend {
		return bytes.Compare(root, p.rootForLeaf(key, zerosHash[:])) == 0
	}
	return false
}

func (p *Proof) Value() []byte {
	if p.value == nil {
		return nil
	}
	rst := make([]byte, len(p.value))
	copy(rst, p.value)
	return rst
}

func (p *Proof) Size() int {
	// 1 byte for type
	// 1 byte for trace length - max trace is 256, min valid trace is 1
	// 32 byte for trace mask - bit is set if hash is not zeros hash
	// 32 * non zeros hashes
	// if collision: 32 bytes hashed key and 4 bytes length prefixed value (TODO replace with hashed)
	// if member: 4 bytes length prefixed value
	// if deadend: 0
	if len(p.trace) == 0 {
		return 0
	}
	psize := 34
	for i := range p.trace {
		if bytes.Compare(p.trace[i], zerosHash[:]) != 0 {
			psize += size
		}
	}
	switch p.ptype {
	case collision:
		psize += size
		psize += 4
		psize += len(p.cval)
	case member:
		psize += 4
		psize += len(p.value)
	case deadend:
	default:
		return 0
	}
	return psize
}

func (p *Proof) Marshal() []byte {
	buf := make([]byte, p.Size())
	p.MarshalTo(buf)
	return buf
}

func (p *Proof) MarshalTo(buf []byte) {
	buf[0] = p.ptype
	buf[1] = byte(len(p.trace) - 1)
	offset := 34
	for i := range p.trace {
		if bytes.Compare(p.trace[i], zerosHash[:]) != 0 {
			setBit32(buf[2:34], i)
			copy(buf[offset:], p.trace[i])
			offset += size
		}
	}
	switch p.ptype {
	case collision:
		copy(buf[offset:], p.ckey)
		offset += size
		order.PutUint32(buf[offset:], uint32(len(p.cval)))
		offset += 4
		copy(buf[offset:], p.cval)
	case member:
		order.PutUint32(buf[offset:], uint32(len(p.value)))
		offset += 4
		copy(buf[offset:], p.value)
	}
}

func (p *Proof) Unmarshal(buf []byte) {
	p.ptype = buf[0]
	p.trace = make([][]byte, int(buf[1])+1)
	offset := 34
	for i := range p.trace {
		if isBitSet32(buf[2:34], i) {
			p.trace[i] = make([]byte, size)
			copy(p.trace[i], buf[offset:])
			offset += size
		} else {
			p.trace[i] = zerosHash[:]
		}
	}
	switch p.ptype {
	case collision:
		p.ckey = make([]byte, size)
		copy(p.ckey, buf[offset:])
		offset += size
		len := int(order.Uint32(buf[offset:]))
		offset += 4
		p.cval = make([]byte, len)
		copy(p.cval, buf[offset:])
	case member:
		len := int(order.Uint32(buf[offset:]))
		offset += 4
		p.value = make([]byte, len)
		copy(p.value, buf[offset:])
	}
}

func setBit32(buf []byte, index int) {
	quo, rem := index/8, index%8
	buf[quo] = buf[quo] | 1<<rem
}

func isBitSet32(buf []byte, index int) bool {
	quo, rem := index/8, index%8
	return (buf[quo] & (1 << rem)) > 0

}
