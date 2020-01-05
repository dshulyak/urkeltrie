package urkeltrie

type Reader interface {
	Get([]byte) ([]byte, error)
	GenerateProof([]byte, *Proof) error
	Version() uint64
	Hash() []byte
	TreeIterator
}

type Snapshot = Reader

type ReadWriter interface {
	Reader
	Put([]byte, []byte) error
	Delete([]byte) error
}

type Committer interface {
	Commit() error
}

type Entry interface {
	Key() ([]byte, error)
	Value() ([]byte, error)
}

type TreeIterator interface {
	Iterate(IterateFunc) error
	ReverseIterate(IterateFunc) error
}

type IterateFunc func(e Entry) bool
