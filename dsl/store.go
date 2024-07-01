package dsl

import (
	"github.com/gernest/rbf/dsl/db"
	"google.golang.org/protobuf/proto"
)

type Store[T proto.Message] struct {
	shards *db.Shards
	ops    *Ops
}
