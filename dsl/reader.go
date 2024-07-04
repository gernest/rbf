package dsl

import (
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/cursor"
	"github.com/gernest/rbf/dsl/query"
	"github.com/gernest/rbf/dsl/tr"
	"github.com/gernest/rbf/quantum"
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
	case protoreflect.EnumKind:
	case protoreflect.StringKind:
	default:
		return nil, fmt.Errorf("field %v does not support Rows", f.Kind())
	}
	views := []string{StandardView}
	if opts != nil && !opts.From.IsZero() && !opts.To.IsZero() {
		views = quantum.ViewsByTimeRange(StandardView, opts.From, opts.To, quantum.TimeQuantum("D"))
	}
	o := roaring64.New()
	for _, shard := range r.ops.Shards(views...) {
		for _, view := range shard.Views {
			err := r.rowsShards(field, view, shard.Shard, o, opts)
			if err != nil {
				return nil, err
			}
		}
	}
	limit := uint64(math.MaxUint64)
	if opts.Limit != 0 {
		limit = opts.Limit
	}
	size := min(limit, o.GetCardinality())
	if n, err := o.Select(size); err == nil {
		o.RemoveRange(n, o.Maximum())
	}
	return query.IDs(o.ToArray()), nil
}

func (r *Reader[T]) rowsShards(field, view string, shard uint64, o *roaring64.Bitmap, opts *RowsOption) error {
	var column uint64
	if opts != nil && opts.Column != 0 {
		colShard := opts.Column >> shardwidth.Exponent
		if colShard != shard {
			return nil
		}
	}
	var limit uint64
	if opts != nil && opts.Limit != 0 {
		limit = opts.Limit
	}
	return r.cursor(ViewKey(field, view), shard, func(c *rbf.Cursor, tr *tr.Read) error {
		filters := []roaring.BitmapFilter{}
		if column != 0 {
			filters = append(filters, roaring.NewBitmapColumnFilter(column))
		}
		if limit != 0 {
			filters = append(filters, roaring.NewBitmapRowLimitFilter(opts.Limit))
		}
		if opts != nil && opts.Like != "" {
			return tr.SearchRe(field, opts.Like, nil, nil, func(_ []byte, value uint64) {
				o.Add(value)
			})
		}
		var start uint64
		if opts != nil {
			start = opts.Previous
		}
		return cursor.Rows(c, start, func(row uint64) error {
			o.Add(row)
			return nil
		}, filters...)
	})
}

func (r *Reader[T]) cursor(view string, shard uint64, f func(c *rbf.Cursor, tr *tr.Read) error) error {
	return r.store.shards.View2(shard, func(tx *rbf.Tx, tr *tr.Read) error {
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
