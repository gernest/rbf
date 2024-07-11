package dsl

import (
	"math"
	"testing"

	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/kase"
	"github.com/gernest/roaring/shardwidth"
	"github.com/stretchr/testify/require"
)

func TestStore(t *testing.T) {
	db, err := New[*kase.Model](t.TempDir())
	require.NoError(t, err)
	defer db.Close()
	db.Append([]*kase.Model{
		{},
		{
			Enum:    kase.Model_one,
			Bool:    true,
			String_: "hello",
			Blob:    []byte("hello"),
			Int64:   math.MaxInt64,
			Uint64:  shardwidth.ShardWidth,
			Double:  math.MaxFloat64,
			Set:     []string{"hello"},
			BlobSet: [][]byte{[]byte("hello")},
		},
	})
	require.NoError(t, db.Flush())
	want := []string{"blob", "blob_set", "bool", "double", "enum", "int64", "set", "string", "uint64"}
	var got []string
	db.db.View(0, func(tx *rbf.Tx) error {
		got = tx.FieldViews()
		return nil
	})
	require.Equal(t, want, got)
}
