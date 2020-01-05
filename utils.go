package urkeltrie

import (
	"hash"
	"hash/crc32"

	"github.com/dshulyak/urkeltrie/store"
	"golang.org/x/crypto/blake2s"
)

func hasher() hash.Hash {
	h, _ := blake2s.New256(nil)
	return h
}

func sum(key []byte) (rst [size]byte) {
	h := hasher()
	h.Write(key)
	h.Sum(rst[:0])
	return
}

type node interface {
	// TODO rework node visibility, majority of this methods shouldn't be visible outside module
	isDirty() bool
	Get([size]byte) ([]byte, error)
	Hash() []byte
	Allocate()
	Position() (uint32, uint32)
	Commit() error
	Prove([size]byte, *Proof) error
	Delete([size]byte) (bool, error)
	Sync() error
}

func appendCrcSum32(crc []byte, bufs ...[]byte) {
	digest := crc32.New(crcTable)
	for _, buf := range bufs {
		digest.Write(buf)
	}
	digest.Sum(crc)
}

func crcSum32(buf []byte) uint32 {
	digest := crc32.New(crcTable)
	digest.Write(buf)
	return digest.Sum32()
}

func marshalVersionTo(version uint64, node *inner, buf []byte) {
	order.PutUint64(buf, version)
	idx, pos := node.Position()
	order.PutUint32(buf[8:], idx)
	order.PutUint32(buf[12:], pos)
	copy(buf[16:], node.Hash())
	appendCrcSum32(buf[48:48], buf[:48])
}

func unmarshalVersion(store *store.FileStore, buf []byte) (uint64, *inner, error) {
	if crcSum32(buf[:48]) != order.Uint32(buf[48:]) {
		return 0, nil, ErrCRC
	}
	var (
		version  uint64
		idx, pos uint32
		hash     = make([]byte, 32)
	)
	version = order.Uint64(buf)
	idx = order.Uint32(buf[8:])
	pos = order.Uint32(buf[12:])
	copy(hash, buf[16:])
	return version, createInner(store, 0, idx, pos, hash), nil
}
