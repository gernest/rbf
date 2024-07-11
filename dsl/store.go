package dsl

import (
	"errors"

	"github.com/gernest/rbf/dsl/db"
	"google.golang.org/protobuf/proto"
)

type Store[T proto.Message] struct {
	db     *db.Shards
	ops    *Ops
	schema *Schema[T]
}

func New[T proto.Message](path string, bsi ...string) (*Store[T], error) {
	o, err := newOps(path)
	if err != nil {
		return nil, err
	}
	db, err := db.New(path)
	if err != nil {
		o.Close()
		return nil, err
	}

	schema, err := NewSchema[T](bsi...)
	if err != nil {
		o.Close()
		db.Close()
		return nil, err
	}

	s := &Store[T]{db: db, ops: o, schema: schema}
	return s, nil
}

func (s *Store[T]) Close() error {
	return errors.Join(s.db.Close(), s.ops.Close())
}

func (s *Store[T]) DB() *db.Shards {
	return s.db
}
