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

type Option[T proto.Message] func(store *Store[T])

func New[T proto.Message](path string, opts ...Option[T]) (*Store[T], error) {
	o, err := newOps(path)
	if err != nil {
		return nil, err
	}
	db, err := db.New(path)
	if err != nil {
		o.Close()
		return nil, err
	}

	schema, err := NewSchema[T]()
	if err != nil {
		o.Close()
		db.Close()
		return nil, err
	}

	s := &Store[T]{db: db, ops: o, schema: schema}
	for i := range opts {
		opts[i](s)
	}
	return s, nil
}

func (s *Store[T]) Close() error {
	return errors.Join(s.db.Close(), s.ops.Close())
}

func (s *Store[T]) DB() *db.Shards {
	return s.db
}
