package urkeltrie

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTreeProve(t *testing.T) {
	rand.Seed(time.Now().Unix())
	tree := setupFullTree(t, 1000)

	key := make([]byte, 20)
	value := make([]byte, 10)
	require.NoError(t, tree.Put(key, value))

	proof := NewProof(256)
	require.NoError(t, tree.GenerateProof(key, proof))
	require.Equal(t, tree.Hash(), proof.RootFor(key, value))
	require.True(t, proof.VerifyMembership(tree.Hash(), key, value))
}

func TestTreeProvePersistent(t *testing.T) {
	tree, closer := setupFullTreeP(t, 4)
	defer closer()

	var (
		key = make([]byte, 10)
	)
	rand.Read(key)
	require.NoError(t, tree.Put(key, key))

	root := tree.Hash()
	require.NoError(t, tree.Commit())

	proof := NewProof(256)
	require.NoError(t, tree.GenerateProof(key, proof))
	require.True(t, proof.VerifyMembership(root, key, key))
}

func BenchmarkGenerateProof(b *testing.B) {
	rand.Seed(time.Now().Unix())
	tree := setupFullTree(b, 10000)

	key := make([]byte, 20)
	value := make([]byte, 10)
	require.NoError(b, tree.Put(key, value))

	proof := NewProof(256)
	// hashes are lazily cached
	require.NoError(b, tree.GenerateProof(key, proof))
	proof.Reset()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		require.NoError(b, tree.GenerateProof(key, proof))
		proof.Reset()
	}
}

func BenchmarkVerifyMembership(b *testing.B) {
	rand.Seed(time.Now().Unix())
	tree := setupFullTree(b, 10000)

	key := make([]byte, 20)
	value := make([]byte, 10)
	require.NoError(b, tree.Put(key, value))

	proof := NewProof(256)
	root := tree.Hash()
	require.NoError(b, tree.GenerateProof(key, proof))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		require.True(b, proof.VerifyMembership(root, key, value))
	}
}

func BenchmarkProveMember500000(b *testing.B) {
	tree, closer := setupFullTreeP(b, 0)
	defer closer()
	ftree := NewFlushTreeFromTree(tree, 500)
	for i := 0; i < 500000; i++ {
		key := make([]byte, 10)
		value := make([]byte, 50)
		rand.Read(key)
		rand.Read(value)
		require.NoError(b, ftree.Put(key, value))
	}

	var (
		keys   = [][]byte{}
		values = [][]byte{}
	)
	for i := 0; i < 100; i++ {
		key := make([]byte, 10)
		value := make([]byte, 100)
		rand.Read(key)
		rand.Read(value)
		require.NoError(b, tree.Put(key, value))
		keys = append(keys, key)
		values = append(values, value)
	}

	proof := NewProof(256)
	root := tree.Hash()
	require.NoError(b, tree.Commit())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index := rand.Intn(len(keys))
		key := keys[index]
		value := values[index]
		require.NoError(b, tree.GenerateProof(key, proof))
		require.NoError(b, tree.LoadLatest())
		require.True(b, proof.VerifyMembership(root, key, value))
		proof.Reset()
	}

}
