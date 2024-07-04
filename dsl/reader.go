package dsl

import (
	"errors"
	"fmt"
	"time"

	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/cursor"
	"github.com/gernest/rbf/dsl/query"
	"github.com/gernest/rbf/dsl/tr"
	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

type Reader[T proto.Message] struct {
	store  *Store[T]
	ops    *readOps
	fields protoreflect.FieldDescriptors
}

type RowsOption struct {
	Like     string
	Column   uint64
	Limit    uint64
	From     time.Time
	To       time.Time
	Previous uint64
}

func (r *Reader[T]) Rows(field string, opts *RowsOption) (query.IDs, error) {
	f := r.fields.ByName(protoreflect.Name(field))
	if f == nil {
		return nil, fmt.Errorf("field %s not found", field)
	}
	switch f.Kind() {
	case protoreflect.EnumKind, protoreflect.StringKind:
	default:
		return nil, fmt.Errorf("field %v does not support Rows", f.Kind())
	}
	var shards []uint64
	if opts != nil && !opts.From.IsZero() && !opts.To.IsZero() {
		shards = r.ops.ShardsRange(opts.From.Format(Quantum), opts.To.Format(Quantum))
	} else {
		shards = r.ops.All()
	}
	limit := int(^uint(0) >> 1)
	if opts != nil && opts.Limit != 0 {
		limit = int(opts.Limit)
	}
	var rowIDs query.IDs
	for _, shard := range shards {
		rs, err := r.RowsShards(f, shard, opts)
		if err != nil {
			return nil, err
		}
		rowIDs = rowIDs.Merge(rs, limit)
	}
	return rowIDs, nil
}

func (r *Reader[T]) RowsShards(field protoreflect.FieldDescriptor, shard uint64, opts *RowsOption) (query.IDs, error) {
	var rowIDs query.IDs
	var column uint64
	if opts != nil && opts.Column != 0 {
		colShard := opts.Column >> shardwidth.Exponent
		if colShard != shard {
			return rowIDs, nil
		}
	}
	var limit uint64
	if opts != nil && opts.Limit != 0 {
		limit = opts.Limit
	}
	err := r.cursor(field, shard, func(c *rbf.Cursor, tr *tr.Read) error {
		filters := []roaring.BitmapFilter{}
		if column != 0 {
			filters = append(filters, roaring.NewBitmapColumnFilter(column))
		}
		if limit != 0 {
			filters = append(filters, roaring.NewBitmapRowLimitFilter(opts.Limit))
		}
		if opts != nil && opts.Like != "" {
			err := tr.SearchRe(string(field.Name()), opts.Like, nil, nil, func(_ []byte, value uint64) {
				filters = append(filters, roaring.NewBitmapColumnFilter(value))
			})
			if err != nil {
				return err
			}
		}
		var start uint64
		if opts != nil {
			start = opts.Previous
		}
		return cursor.Rows(c, start, func(row uint64) error {
			rowIDs = append(rowIDs, row)
			return nil
		}, filters...)
	})
	if err != nil {
		return nil, err
	}
	return rowIDs, nil
}

func (r *Reader[T]) cursor(field protoreflect.FieldDescriptor, shard uint64, f func(c *rbf.Cursor, tr *tr.Read) error) error {
	return r.store.shards.View2(shard, func(tx *rbf.Tx, tr *tr.Read) error {
		view := ViewKey(string(field.Name()), StandardView)
		c, err := tx.Cursor(view)
		if err != nil {
			if errors.Is(err, rbf.ErrBitmapNotFound) {
				return nil
			}
			return err
		}
		defer c.Close()
		return f(c, tr)
	})
}
