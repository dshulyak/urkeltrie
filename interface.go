package urkeltrie

type Reader interface {
	Get([]byte) ([]byte, error)
	GenerateProof([]byte, *Proof) error
	Version() uint64
	Hash() []byte
}

type Snapshot = Reader

type ReadWriter interface {
	Reader
	Put([]byte, []byte) error
	Delete([]byte) error
}

type Committer interface {
	ReadWriter
	Commit() error
}
