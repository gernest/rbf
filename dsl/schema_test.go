package dsl

import (
	"testing"

	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/cursor"
	"github.com/gernest/rbf/dsl/kase"
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
