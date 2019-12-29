package urkeltrie

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

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
	store, err := NewFileStore("")
	require.NoError(b, err)
	tree := NewTree(store)
	var (
		key   = make([]byte, 20)
		value = make([]byte, 10)
	)
	for i := 0; i < 10000; i++ {
		rand.Read(key)
		rand.Read(value)
		require.NoError(b, tree.Put(key, value))
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rand.Read(key)
		tree.Get(key)
	}
}
