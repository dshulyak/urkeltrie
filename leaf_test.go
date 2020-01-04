package urkeltrie

import (
	"errors"
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
	require.NoError(t, l2.Unmarshal(buf))

	// remove for comparison
	l1.value = nil

	require.Equal(t, l1, l2)
}

func TestLeafCorrupted(t *testing.T) {
	l1 := &leaf{
		key:         [size]byte{1, 2, 3},
		value:       make([]byte, 7),
		valueLength: 7,
		valueIdx:    1,
		valuePos:    18,
	}
	buf := l1.Marshal()
	buf[7] ^= 0xff

	require.True(t, errors.Is(new(leaf).Unmarshal(buf), ErrCRC))

	buf[7] ^= 0xff
	require.Nil(t, new(leaf).Unmarshal(buf))
}
