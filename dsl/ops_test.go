package dsl

import (
	"testing"

	"github.com/gernest/roaring"
	"github.com/stretchr/testify/require"
)

func TestOps(t *testing.T) {
	o, err := newOps(t.TempDir())
	require.NoError(t, err)
	defer o.db.Close()

	w, err := o.write()
	require.NoError(t, err)

	require.NoError(t, w.Commit(map[string]*roaring.Bitmap{
		"test": roaring.NewBitmap(1, 2, 3),
		"1":    roaring.NewBitmap(1),
		"2":    roaring.NewBitmap(2),
		"3":    roaring.NewBitmap(3),
	}))

	r, err := o.read()
	require.NoError(t, err)

	defer r.Release()
	require.Equal(t, []Shard{
		{1, []string{"1", "test"}},
		{2, []string{"test"}},
		{3, []string{"3", "test"}},
	}, r.Shards("test", "1", "3"))
}
