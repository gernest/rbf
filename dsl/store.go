package dsl

import (
	"errors"

	"github.com/gernest/rbf/dsl/db"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Store[T proto.Message] struct {
	db             *db.Shards
	ops            *Ops
	timestampField protoreflect.Name
}

type Option[T proto.Message] func(store *Store[T])

func WithTimestampField[T proto.Message](name string) Option[T] {
	return func(store *Store[T]) {
		store.timestampField = protoreflect.Name(name)
	}
}
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

	s := &Store[T]{db: db, ops: o, timestampField: TimestampField}
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
