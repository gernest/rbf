package dsl

import (
	"errors"

	"github.com/gernest/rbf/dsl/db"
	"google.golang.org/protobuf/proto"
)

type Store[T proto.Message] struct {
	shards *db.Shards
	ops    *Ops
}

func New[T proto.Message](path string) (*Store[T], error) {
	o, err := newOps(path)
	if err != nil {
		return nil, err
	}
	return &Store[T]{shards: db.New(path), ops: o}, nil
}

func (s *Store[T]) Close() error {
	return errors.Join(s.shards.Close(), s.ops.db.Close())
}

func (s *Store[T]) DB() *db.Shards {
	return s.shards
}
