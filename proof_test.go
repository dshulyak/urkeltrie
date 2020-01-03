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
	require.True(t, proof.VerifyMembership(tree.Hash(), key))
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
	require.Equal(t, key, proof.Value())
	require.True(t, proof.VerifyMembership(root, key))
}

func TestProveCollision(t *testing.T) {
	tree := setupFullTree(t, 0)
	value := []byte("testcollision")

	for i := 0; i < 8; i++ {
		key := [size]byte{}
		key[0] = 1 << i
		require.NoError(t, tree.PutRaw(key, value))
	}

	key := [size]byte{}
	key[0] = 0b00001111
	require.NoError(t, tree.PutRaw(key, value))
	root := tree.Hash()

	require.NoError(t, tree.Commit())

	ckey := [size]byte{}
	ckey[0] = 0b10001111
	proof := NewProof(0)
	require.NoError(t, tree.GenerateProofRaw(ckey, proof))
	require.False(t, proof.VerifyMembershipRaw(root, key))
	require.True(t, proof.VerifyNonMembershipRaw(root, key))
}

func TestProveDeadend(t *testing.T) {
	tree := setupFullTree(t, 0)
	value := []byte("testdeadend")

	order := []byte{
		0b00000000,
		0b00000011,
		0b00001011,
		0b00000001,
	}
	for i := range order {
		key := [size]byte{}
		key[0] = order[i]
		require.NoError(t, tree.PutRaw(key, value))
	}

	key := [size]byte{}
	key[0] = 0b00000111
	root := tree.Hash()

	require.NoError(t, tree.Commit())
	proof := NewProof(0)
	require.NoError(t, tree.GenerateProofRaw(key, proof))
	require.False(t, proof.VerifyMembershipRaw(root, key))
	require.True(t, proof.VerifyNonMembershipRaw(root, key))
}

func TestProofMarshal(t *testing.T) {
	tree := setupFullTree(t, 100)
	root := tree.Hash()
	proof := NewProof(0)
	key := make([]byte, 20)
	for i := 0; i < 10; i++ {
		rand.Read(key)
		require.NoError(t, tree.GenerateProof(key, proof))

		member := proof.VerifyMembership(root, key)
		non := proof.VerifyNonMembership(root, key)

		buf := proof.Marshal()

		marshalled := NewProof(0)
		marshalled.Unmarshal(buf)

		require.Equal(t, proof, marshalled)

		require.Equal(t, member, marshalled.VerifyMembership(root, key))
		require.Equal(t, non, marshalled.VerifyNonMembership(root, key))

		proof.Reset()
	}
}

func BenchmarkProveMember500000(b *testing.B) {
	tree, closer := setupProdTree(b)
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
		require.NoError(b, tree.GenerateProof(key, proof))
		require.NoError(b, tree.LoadLatest())
		require.True(b, proof.VerifyMembership(root, key))
		proof.Reset()
	}

}
