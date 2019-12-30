package urkeltrie

import (
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
		bit:   1,
		left:  createInner(nil, 1, 12, h1),
		right: createInner(nil, 2, 20, h2),
	}
	buf := i1.Marshal()
	i2 := &inner{}
	i2.Unmarshal(buf)
	require.Equal(t, i1, i2)
}
