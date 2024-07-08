package dsl

import (
	"time"

	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/tr"
	"github.com/gernest/rbf/dsl/tx"
	"github.com/gernest/rbf/quantum"
	"google.golang.org/protobuf/proto"
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

// Range returns shards for the time range. We select possible views to read
// from based on start and end.
//
// We Store Hour, Day, Month and Year Quantum.
func (r Reader[T]) Range(start, end time.Time) []Shard {
	if end.Equal(start) {
		return r.ops.Shards(quantum.ViewByTimeUnit(StandardView, start, 'H'))
	}
	start = start.Truncate(time.Hour)
	end = end.Truncate(time.Hour)
	diff := end.Sub(start)
	switch {
	case end.Equal(start):
		return r.ops.Shards(quantum.ViewByTimeUnit(StandardView, start, 'H'))
	case diff < (12 * time.Hour):
		return r.RangeUnit(start, end, 'H')
	case diff < (24 * 15 * time.Hour):
		return r.RangeUnit(start, end, 'D')
	case diff < (24 * 30 * 6 * time.Hour):
		return r.RangeUnit(start, end, 'M')
	default:
		return r.RangeUnit(start, end, 'Y')
	}
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
