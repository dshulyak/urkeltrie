package urkeltrie

import "sync"

type SafeTree struct {
	mu   sync.Mutex // this is temporary, concurrency will be adressed separately
	tree *Tree
	// TODO it should copy values that are kept in tree's memory
}

func (s *SafeTree) Get(key []byte) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tree.Get(key)
}

func (s *SafeTree) Put(key, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tree.Put(key, value)
}

func (s *SafeTree) GenerateProof(key []byte, proof *Proof) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tree.GenerateProof(key, proof)
}

func (s *SafeTree) Commit() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tree.Commit()
}

func (s *SafeTree) Hash() []byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tree.Hash()
}

func (s *SafeTree) LoadLatest() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tree.LoadLatest()
}

func (s *SafeTree) LoadVersion(version uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tree.LoadVersion(version)
}

func (s *SafeTree) Version() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tree.Version()
}

func (s *SafeTree) Snapshot() Snapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tree.Snapshot()
}

func (s *SafeTree) VersionSnapshot(version uint64) (Snapshot, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.tree.VersionSnapshot(version)
}
