package urkeltrie

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIteratorInorder(t *testing.T) {
	tree := setupFullTree(t, 10)
	iter := NewIterator(tree)
	defer iter.Close()

	count := 0
	for ; iter.Valid(); iter.Next() {
		count++
	}
	require.Equal(t, 10, count)
}

func TestIteratorClose(t *testing.T) {
	tree := setupFullTree(t, 10)
	iter := NewIterator(tree)
	defer iter.Close()

	count := 0
	for ; iter.Valid(); iter.Next() {
		count++
		if count == 5 {
			break
		}
	}
	iter.Close()
	require.False(t, iter.Valid())
	require.Panics(t, iter.Next)
}
