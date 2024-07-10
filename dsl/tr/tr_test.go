package tr

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/blevesearch/vellum"
	"github.com/stretchr/testify/require"
)

func TestTr(t *testing.T) {
	f := New(filepath.Join(t.TempDir(), "translate"))
	require.NoError(t, f.Open())
	defer f.Close()
	w, err := f.Write()
	require.NoError(t, err)
	field := "string"
	got := make([]uint64, 5)
	tr, err := w.String(field)
	require.NoError(t, err)
	for n := range got {
		got[n], _ = tr.Tr([]byte(strconv.Itoa(n)))
	}
	require.NoError(t, w.Commit())
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, got)

	t.Run("Dupe", func(t *testing.T) {
		w, err := f.Write()
		require.NoError(t, err)
		got2 := make([]uint64, 5)
		tr, err := w.String(field)
		require.NoError(t, err)
		for n := range got2 {
			got2[n], _ = tr.Tr([]byte(strconv.Itoa(n)))
		}
		require.NoError(t, w.Commit())
		require.Equal(t, got, got2)
	})

	t.Run("Key", func(t *testing.T) {
		r, err := f.Read()
		require.NoError(t, err)
		defer r.Release()
		got := make([]string, 5)
		for n := range got {
			got[n] = string(r.Key(field, uint64(n+1)))
		}
		want := []string{"0", "1", "2", "3", "4"}
		require.Equal(t, want, got)
	})

	t.Run("Find", func(t *testing.T) {
		r, err := f.Read()
		require.NoError(t, err)
		defer r.Release()
		got := make([]uint64, 5)
		for n := range got {
			got[n], _ = r.Find(field, []byte(strconv.Itoa(n+1)))
		}
		want := []uint64{0x2, 0x3, 0x4, 0x5, 0x0}
		require.Equal(t, want, got)
	})

	t.Run("Search", func(t *testing.T) {
		r, err := f.Read()
		require.NoError(t, err)
		defer r.Release()

		got := map[string]uint64{}
		err = r.Search(field, &vellum.AlwaysMatch{}, nil, nil, func(key []byte, value uint64) error {
			got[string(key)] = value
			return nil
		})
		require.NoError(t, err)
		want := map[string]uint64{"0": 0x1, "1": 0x2, "2": 0x3, "3": 0x4, "4": 0x5}
		require.Equal(t, want, got)
	})
}

func TestTr_empty(t *testing.T) {
	f := New(filepath.Join(t.TempDir(), "translate"))
	require.NoError(t, f.Open())
	defer f.Close()
	w, err := f.Write()
	require.NoError(t, err)
	defer w.Commit()
	field := "string"
	tr, err := w.String(field)
	require.NoError(t, err)

	id, err := tr.Tr([]byte(""))
	require.NoError(t, err)

	require.Equal(t, uint64(1), id)

	require.NoError(t, w.Commit())

	r, err := f.Read()
	require.NoError(t, err)
	defer r.Release()
	require.Empty(t, r.Key(field, id))
}

func TestBlob(t *testing.T) {
	f := New(filepath.Join(t.TempDir(), "translate"))
	require.NoError(t, f.Open())
	defer f.Close()
	w, err := f.Write()
	require.NoError(t, err)
	field := "string"
	tr, err := w.Blobs(field)
	require.NoError(t, err)
	got := make([]uint64, 5)
	for n := range got {
		got[n], _ = tr.Tr([]byte(strconv.Itoa(n)))
	}
	require.NoError(t, w.Commit())
	require.Equal(t, []uint64{1, 2, 3, 4, 5}, got)

	t.Run("Dupe", func(t *testing.T) {
		w, err := f.Write()
		require.NoError(t, err)
		got2 := make([]uint64, 5)
		tr, err := w.Blobs(field)
		require.NoError(t, err)
		for n := range got2 {
			got2[n], _ = tr.Tr([]byte(strconv.Itoa(n)))
		}
		require.NoError(t, w.Commit())
		require.Equal(t, got, got2)
	})

	t.Run("Search is disabled", func(t *testing.T) {
		r, err := f.Read()
		require.NoError(t, err)
		defer r.Release()

		got := map[string]uint64{}
		err = r.Search(field, &vellum.AlwaysMatch{}, nil, nil, func(key []byte, value uint64) error {
			got[string(key)] = value
			return nil
		})
		require.NoError(t, err)
		want := map[string]uint64{}
		require.Equal(t, want, got)
	})
	t.Run("FindBlob", func(t *testing.T) {
		r, err := f.Read()
		require.NoError(t, err)
		defer r.Release()
		id, ok := r.FindBlob(field, []byte("3"))
		require.True(t, ok)
		require.Equal(t, uint64(4), id)
	})
}
