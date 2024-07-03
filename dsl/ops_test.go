package dsl

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOps(t *testing.T) {
	o, err := newOps(t.TempDir())
	require.NoError(t, err)
	defer o.db.Close()

	w, err := o.write()
	require.NoError(t, err)

	require.NoError(t, w.Commit(map[string][]uint64{
		"test": {1, 2, 3},
	}))

	r, err := o.read()
	require.NoError(t, err)

	defer r.Release()
	require.Equal(t, []uint64{1, 2, 3}, r.Shards("test"))
}
