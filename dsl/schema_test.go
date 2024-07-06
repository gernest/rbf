package dsl

import (
	"testing"
	"time"

	"github.com/blevesearch/vellum"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/bsi"
	"github.com/gernest/rbf/dsl/cursor"
	"github.com/gernest/rbf/dsl/kase"
	"github.com/gernest/rbf/dsl/tx"
	"github.com/gernest/rows"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"
)

func TestSchemaMutex(t *testing.T) {
	suite.Run(t, newMutex())
}

type Mutex struct {
	Basic[*kase.Mutex]
}

func newMutex() *Mutex {
	return &Mutex{
		Basic: Basic[*kase.Mutex]{
			source: []*kase.Mutex{
				{Mutex: kase.Mutex_zero},
				{Mutex: kase.Mutex_one},
			},
		},
	}
}

type Basic[T proto.Message] struct {
	suite.Suite
	db     *Store[T]
	source []T
}

func (b *Basic[T]) SetupTest() {
	var err error
	b.db, err = New[T](b.T().TempDir())
	b.Require().NoError(err)
}

func (b *Basic[T]) TearDownTest() {
	b.Require().NoError(b.db.Close())
}

func (b *Basic[T]) TestGenerateID() {
	schema, err := b.db.Schema()
	b.Require().NoError(err)
	defer schema.Release()
	want := make([]uint64, len(b.source))
	for i := range b.source {
		schema.Write(b.source[i])
		want[i] = uint64(i + 1)
	}
	b.Require().NoError(schema.Save())
	var ids []uint64
	err = b.db.DB().View(0, func(tx *rbf.Tx) error {
		c, err := tx.Cursor(ViewKey(ID, StandardView))
		if err != nil {
			return err
		}
		defer c.Close()

		r, err := cursor.Row(c, 0, 0)
		if err != nil {
			return err
		}
		ids = r.Columns()
		return nil
	})
	b.Require().NoError(err)
	b.Require().Equal(want, ids)
}

func (b *Basic[T]) TestMultipleShards() {
	schema, err := b.db.Schema()
	b.Require().NoError(err)
	defer schema.Release()
	shards := make([]uint64, len(b.source))
	want := make([]Shard, len(b.source))
	wantID := make([][]uint64, len(b.source))
	for i := range b.source {
		id := uint64(i * rbf.ShardWidth)
		shards[i] = uint64(id / rbf.ShardWidth)
		want[i] = Shard{
			Shard: shards[i],
			Views: []string{StandardView},
		}
		wantID[i] = []uint64{id}
		schema.write(id, b.source[i].ProtoReflect())
	}
	b.Require().NoError(schema.Save())
	r, err := b.db.ops.read()
	b.Require().NoError(err)
	defer r.Release()
	b.Require().Equal(want, r.Shards(StandardView))
	ids := [][]uint64{}
	for i := range shards {
		err = b.db.DB().View(shards[i], func(tx *rbf.Tx) error {
			c, err := tx.Cursor(ViewKey(ID, StandardView))
			if err != nil {
				return err
			}
			defer c.Close()

			r, err := cursor.Row(c, shards[i], 0)
			if err != nil {
				return err
			}
			ids = append(ids, r.Columns())
			return nil
		})
		b.Require().NoError(err, shards[i])
	}
	b.Require().Equal(wantID, ids)
}

func TestString(t *testing.T) {
	suite.Run(t, newString())
}

type StringTest struct {
	Basic[*kase.String]
}

func newString() *StringTest {
	return &StringTest{
		Basic: Basic[*kase.String]{
			source: []*kase.String{
				{String_: "hello"},
				{String_: "world"},
				{String_: ""},
			},
		},
	}
}

func (s *StringTest) TestVellum() {

	schema, err := s.db.Schema()
	s.Require().NoError(err)
	for i := range s.source {
		schema.Write(s.source[i])
	}
	s.Require().NoError(schema.Save())

	r, err := s.db.Reader()
	s.Require().NoError(err)
	defer r.Release()
	m := map[string]uint64{}
	err = r.Tr().Search("string", &vellum.AlwaysMatch{}, nil, nil, func(key []byte, value uint64) {
		m[string(key)] = value
	})
	s.Require().NoError(err)
	want := map[string]uint64{
		"hello": 1,
		"world": 2,
	}
	s.Require().Equal(want, m)
}

func TestStringSet(t *testing.T) {
	suite.Run(t, newStringSet())
}

type StringSetTest struct {
	Basic[*kase.StringSet]
}

func newStringSet() *StringSetTest {
	return &StringSetTest{
		Basic: Basic[*kase.StringSet]{
			source: []*kase.StringSet{
				{String_: []string{"hello"}},
				{String_: []string{"world"}},
				{},
			},
		},
	}
}

func (s *StringSetTest) TestVellum() {

	schema, err := s.db.Schema()
	s.Require().NoError(err)
	for i := range s.source {
		schema.Write(s.source[i])
	}
	s.Require().NoError(schema.Save())

	r, err := s.db.Reader()
	s.Require().NoError(err)
	defer r.Release()
	m := map[string]uint64{}
	err = r.Tr().Search("string", &vellum.AlwaysMatch{}, nil, nil, func(key []byte, value uint64) {
		m[string(key)] = value
	})
	s.Require().NoError(err)
	want := map[string]uint64{
		"hello": 1,
		"world": 2,
	}
	s.Require().Equal(want, m)
}

func TestTimeseries(t *testing.T) {
	db, err := New[*kase.TimestampMS](t.TempDir())
	require.NoError(t, err)
	defer db.Close()
	ts := time.Date(2000, time.January, 2, 3, 4, 5, 6, time.UTC)

	err = db.Append([]*kase.TimestampMS{
		{Timestamp: ts.UnixMilli()},
	})
	require.NoError(t, err)

	r, err := db.Reader()
	require.NoError(t, err)
	defer r.Release()

	want := []Shard{
		{Shard: 0x0, Views: []string{"standard", "standard_2000", "standard_200001", "standard_20000102", "standard_2000010203"}},
	}
	require.Equal(t, want, r.ops.All())

	// make sure ID is set for all views
	m := map[string][]uint64{}
	err = r.View(want[0], func(txn *tx.Tx) error {
		return txn.Cursor(ID, func(c *rbf.Cursor, tx *tx.Tx) error {
			r, err := cursor.Row(c, want[0].Shard, 0)
			if err != nil {
				return err
			}
			m[tx.View] = r.Columns()
			return nil
		})
	})
	require.NoError(t, err)
	ids := map[string][]uint64{
		"standard":            {0x1},
		"standard_2000":       {0x1},
		"standard_200001":     {0x1},
		"standard_20000102":   {0x1},
		"standard_2000010203": {0x1},
	}
	require.Equal(t, ids, m)

	// make sure timestamp field is set on all views
	tsv := map[string][]int64{}
	err = r.View(want[0], func(txn *tx.Tx) error {
		return txn.Cursor("timestamp", func(c *rbf.Cursor, tx *tx.Tx) error {
			return bsi.Extract(c, tx.Shard, rows.NewRow(1), func(column uint64, value int64) error {
				tsv[tx.View] = []int64{value}
				return nil
			})
		})
	})
	require.NoError(t, err)
	value := ts.UnixMilli()
	wantTs := map[string][]int64{
		"standard":            {value},
		"standard_2000":       {value},
		"standard_200001":     {value},
		"standard_20000102":   {value},
		"standard_2000010203": {value},
	}
	require.Equal(t, wantTs, tsv)
}
