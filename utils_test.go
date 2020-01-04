package urkeltrie

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVersionCorrupted(t *testing.T) {
	root := &inner{
		pos: 11,
		idx: 157,
	}
	version := uint64(157)
	buf := make([]byte, versionSize)
	marshalVersionTo(version, root, buf)

	buf[1] ^= 0xff
	_, _, err := unmarshalVersion(nil, buf)
	require.True(t, errors.Is(err, ErrCRC))

	buf[1] ^= 0xff

	nversion, n, err := unmarshalVersion(nil, buf)
	require.NoError(t, err)
	require.Equal(t, version, nversion)
	require.Equal(t, root, n)
}
