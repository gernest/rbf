package dsl

import (
	"time"

	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/tr"
	"github.com/gernest/rbf/dsl/tx"
	"github.com/gernest/rbf/quantum"
	"github.com/gernest/roaring/shardwidth"
	"google.golang.org/protobuf/proto"
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
	return &Reader[T]{store: s, ops: r}, nil
}

type Reader[T proto.Message] struct {
	store *Store[T]
	ops   *readOps
}

func (r *Reader[T]) Release() error {
	return r.ops.Release()
}

// Standard returns the standard shard. Use this to query global data
func (r Reader[T]) Standard() Shard {
	a := r.ops.Shards(StandardView)
	if len(a) > 0 {
		return a[0]
	}
	return Shard{}
}

func (r *Reader[T]) Tr() *tr.Read {
	return r.ops.tr
}

// Range returns shards for the time range.
func (r Reader[T]) Range(start, end time.Time) []Shard {
	return r.RangeUnit(start, end, 'D')
}

func (r Reader[T]) RangeUnit(start, end time.Time, unit rune) []Shard {
	if sameDate(start, end) {
		return r.ops.Shards(quantum.ViewByTimeUnit(StandardView, start, unit))
	}
	views := quantum.ViewsByTimeRange(StandardView, start, end, quantum.TimeQuantum(unit))
	return r.ops.Shards(views...)
}

func (r *Reader[T]) View(shard Shard, f func(txn *tx.Tx) error) error {
	return r.store.db.View(shard.Shard, func(txn *rbf.Tx) error {
		rx, err := r.store.ops.read()
		if err != nil {
			return err
		}
		defer rx.Release()
		for _, view := range shard.Views {
			err := f(&tx.Tx{
				Tx:    txn,
				Shard: shard.Shard,
				View:  view,
				Tr:    rx.tr,
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func sameDate(a, b time.Time) bool {
	y, m, d := a.Date()
	yy, mm, dd := b.Date()
	return dd == d && mm == m && yy == y
}
