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
