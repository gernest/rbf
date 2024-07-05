package dsl

import (
	"errors"

	"github.com/gernest/rbf/dsl/db"
	"google.golang.org/protobuf/proto"
)

type Store[T proto.Message] struct {
	db  *db.Shards
	ops *Ops
}

func New[T proto.Message](path string) (*Store[T], error) {
	o, err := newOps(path)
	if err != nil {
		return nil, err
	}
	db, err := db.New(path)
	if err != nil {
		o.Close()
		return nil, err
	}

	return &Store[T]{db: db, ops: o}, nil
}

func (s *Store[T]) Close() error {
	return errors.Join(s.db.Close(), s.ops.Close())
}

func (s *Store[T]) DB() *db.Shards {
	return s.db
}
