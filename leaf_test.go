package urkeltrie

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLeafUnmarshal(t *testing.T) {
	l1 := &leaf{
		key:         [size]byte{1, 2, 3},
		value:       make([]byte, 10),
		valueLength: 10,
		valueIdx:    87,
		valuePos:    17,
	}
	l2 := &leaf{}
	buf := l1.Marshal()
	l2.Unmarshal(buf)

	// remove for comparison
	l1.value = nil

	require.Equal(t, l1, l2)
}
