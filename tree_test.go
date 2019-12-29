package urkeltrie

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func setupFullTree(tb testing.TB, hint int) *Tree {
	store, err := NewFileStore("")
	require.NoError(tb, err)
	tree := NewTree(store)
	var (
		key   = make([]byte, 20)
		value = make([]byte, 10)
	)
	for i := 0; i < hint; i++ {
		rand.Read(key)
		rand.Read(value)
		require.NoError(tb, tree.Put(key, value))
	}
	return tree
}

func TestTreeGet(t *testing.T) {
	rand.Seed(time.Now().Unix())
	store, err := NewFileStore("")
	require.NoError(t, err)
	tree := NewTree(store)
	var (
		added  = [][]byte{}
		values = [][]byte{}
	)
	for i := 0; i < 1000; i++ {
		key := make([]byte, 10)
		value := make([]byte, 5)
		rand.Read(key)
		rand.Read(value)
		require.NoError(t, tree.Put(key, value))
		added = append(added, key)
		values = append(values, value)
	}
	for i, key := range added {
		rst, err := tree.Get(key)
		require.NoError(t, err)
		require.Equal(t, values[i], rst)
	}
}

func BenchmarkTreeGet(b *testing.B) {
	tree := setupFullTree(b, 10000)
	b.ResetTimer()
	key := make([]byte, 10)
	for i := 0; i < b.N; i++ {
		rand.Read(key)
		_, _ = tree.Get(key)
	}
}
