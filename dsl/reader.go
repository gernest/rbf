package dsl

import (
	"errors"
	"time"

	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/cursor"
	"github.com/gernest/rbf/dsl/tr"
	"github.com/gernest/rbf/quantum"
	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	// shardVsContainerExponent is the power of 2 of ShardWith minus the power
	// of two of roaring container width (which is 16).
	// 2^shardVsContainerExponent is the number of containers in a shard row.
	//
	// It is represented in this rather awkward way because calculating the row
	// which a given container is in means dividing by the number of rows per
	// container which is performantly expressed as a right shift by this
	// exponent.
	shardVsContainerExponent = shardwidth.Exponent - 16
)

// Reader creates a new Reader for querying the store T. Make sure the reader is
// released after use.
func (s *Store[T]) Reader() (*Reader[T], error) {
	r, err := s.ops.read()
	if err != nil {
		return nil, err
	}
	var a T
	return &Reader[T]{store: s, ops: r, fields: a.ProtoReflect().Descriptor().Fields()}, nil
}

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

// Standard returns the standard shard. Use this to query global data
func (r Reader[T]) Standard() Shard {
	a := r.ops.Shards(StandardView)
	if len(a) > 0 {
		return a[0]
	}
	return Shard{}
}

// Range returns shards for the time range.
func (r Reader[T]) Range(start, end time.Time) []Shard {
	if sameDate(start, end) {
		return r.ops.Shards(quantum.ViewByTimeUnit(StandardView, start, 'D'))
	}
	views := quantum.ViewsByTimeRange(StandardView, start, end, "D")
	return r.ops.Shards(views...)
}

func sameDate(a, b time.Time) bool {
	y, m, d := a.Date()
	yy, mm, dd := b.Date()
	return dd == d && mm == m && yy == y
}

func (r *Reader[T]) Rows(field, view string, shard uint64, o *roaring64.Bitmap, opts *RowsOption) error {
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

func (r *Reader[T]) Distinct(field, view string, shard uint64, o *roaring64.Bitmap, filterBitmap *roaring.Bitmap) error {
	return r.cursor(ViewKey(field, view), shard, func(c *rbf.Cursor, tr *tr.Read) error {
		fragData := c.Iterator()

		// We can't grab the containers "for each row" from the set-type field,
		// because we don't know how many rows there are, and some of them
		// might be empty, so really, we're going to iterate through the
		// containers, and then intersect them with the filter if present.
		var filter []*roaring.Container
		if filterBitmap != nil {
			filter = make([]*roaring.Container, 1<<shardVsContainerExponent)
			filterIterator, _ := filterBitmap.Containers.Iterator(0)
			// So let's get these all with a nice convenient 0 offset...
			for filterIterator.Next() {
				k, c := filterIterator.Value()
				if c.N() == 0 {
					continue
				}
				filter[k%(1<<shardVsContainerExponent)] = c
			}
		}
		prevRow := ^uint64(0)
		seenThisRow := false
		for fragData.Next() {
			k, c := fragData.Value()
			row := k >> shardVsContainerExponent
			if row == prevRow {
				if seenThisRow {
					continue
				}
			} else {
				seenThisRow = false
				prevRow = row
			}
			if filterBitmap != nil {
				if roaring.IntersectionAny(c, filter[k%(1<<shardVsContainerExponent)]) {
					o.Add(row)
					seenThisRow = true
				}
			} else if c.N() != 0 {
				o.Add(row)
				seenThisRow = true
			}
		}
		return nil
	})
}

func (r *Reader[T]) cursor(view string, shard uint64, f func(c *rbf.Cursor, tr *tr.Read) error) error {
	return r.store.db.View(shard, func(tx *rbf.Tx) error {
		c, err := tx.Cursor(view)
		if err != nil {
			if errors.Is(err, rbf.ErrBitmapNotFound) {
				return nil
			}
			return err
		}
		defer c.Close()
		return f(c, r.ops.tr)
	})
}
