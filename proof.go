package urkeltrie

func NewProof(hint int) *Proof {
	return &Proof{
		Trace: make([]ProofNode, 0, hint),
	}
}

type Proof struct {
	Trace []ProofNode
}

func (p *Proof) Reset() {
	p.Trace = p.Trace[:0]
}

func (p *Proof) AppendLeft(hash [size]byte) {
	p.Trace = append(p.Trace, ProofNode{Hash: hash, Left: true})
}

func (p *Proof) AppendRight(hash [size]byte) {
	p.Trace = append(p.Trace, ProofNode{Hash: hash})
}

func (p *Proof) RootFor(key, value []byte) (rst [size]byte) {
	h := hasher()
	rst = leafHash(sum(key), value)
	lth := len(p.Trace) - 1
	for i := range p.Trace {
		sibling := p.Trace[lth-i]
		h.Write([]byte{innerDomain})
		if sibling.Left {
			h.Write(sibling.Hash[:])
			h.Write(rst[:])
		} else {
			h.Write(rst[:])
			h.Write(sibling.Hash[:])
		}
		h.Sum(rst[:0])
		h.Reset()
	}
	return
}

func (p *Proof) VerifyMembership(root [size]byte, key, value []byte) bool {
	return root == p.RootFor(key, value)
}

type ProofNode struct {
	Hash [size]byte
	Left bool
}
