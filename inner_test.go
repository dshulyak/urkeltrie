package urkeltrie

import (
	"errors"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestInnerMarshal(t *testing.T) {
	rand.Seed(time.Now().Unix())
	h1 := make([]byte, 32)
	h2 := make([]byte, 32)
	rand.Read(h1)
	rand.Read(h2)
	i1 := &inner{
		bit:   9,
		left:  createInner(10, 1, 12, h1),
		right: createInner(10, 2, 20, h2),
	}
	buf := i1.Marshal()
	i2 := &inner{bit: 9}
	require.NoError(t, i2.Unmarshal(buf))
	require.Equal(t, i1, i2)
}

func TestInnerCorrupted(t *testing.T) {
	h1 := make([]byte, 32)
	rand.Read(h1)
	i1 := &inner{
		bit:  9,
		left: createInner(1, 12, 12, h1),
	}
	buf := i1.Marshal()

	buf[10] ^= 0xff
	require.True(t, errors.Is(new(inner).Unmarshal(buf), ErrCRC))

	buf[10] ^= 0xff
	require.NoError(t, new(inner).Unmarshal(buf))
}
