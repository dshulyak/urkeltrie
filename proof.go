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

	// hashed collision key and value
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
	for i := len(p.trace) - 1; i >= 0; i-- {
		sibling := p.trace[i]
		h.Write([]byte{innerDomain})
		if bitSet(lkey, i) {
			h.Write(sibling)
			h.Write(leaf)
		} else {
			h.Write(leaf)
			h.Write(sibling)
		}
		leaf = h.Sum(leaf[:0])
		h.Reset()
	}
	return leaf
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
