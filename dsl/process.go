package dsl

import (
	"context"
	"fmt"
	"time"

	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/schema"
	"github.com/gernest/rbf/dsl/tr"
	"github.com/gernest/rbf/quantum"
	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
	"google.golang.org/protobuf/proto"
)

func (s *Store[T]) Process(ctx context.Context, data <-chan T) error {
	w, err := s.write()
	if err != nil {
		return err
	}
	defer w.Release()

	tick := time.NewTicker(time.Minute)
	defer tick.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-data:
			err := w.append(e)
			if err != nil {
				return err
			}
		case ts := <-tick.C:
			err = w.save(ts)
			if err != nil {
				return err
			}
		}
	}
}

type writer[T proto.Message] struct {
	store   *Store[T]
	schema  *schema.Schema[T]
	ox      *ops
	wx      *tr.Write
	shard   uint64
	current uint64
	count   uint64
	ts      time.Time
}

func (w *writer[T]) Release() error {
	err := w.save(time.Now())
	if w.ox != nil {
		w.ox.tx.Rollback()
	}
	if w.wx != nil {
		w.wx.Release()
	}
	return err
}

func (s *Store[T]) write() (*writer[T], error) {
	schema, err := schema.NewSchema[T](nil)
	if err != nil {
		return nil, err
	}
	return &writer[T]{
		store:   s,
		schema:  schema,
		current: ^uint64(0),
	}, nil
}

func (w *writer[T]) next() (uint64, error) {
	if w.ox == nil {
		var err error
		w.ox, err = w.store.ops.ops()
		if err != nil {
			return 0, err
		}
	}
	nxt, err := w.ox.NextID()
	if err != nil {
		return 0, err
	}
	w.shard = nxt / shardwidth.ShardWidth
	return nxt, nil
}

func (w *writer[T]) append(e T) error {
	id, err := w.next()
	if err != nil {
		return err
	}
	if w.current != w.shard {
		if w.count != 0 {
			// We have changed shards, save the last shard and reset state. Saving
			// views is per time observed by the server.
			ts := w.ts
			if ts.IsZero() {
				ts = time.Now()
			}
			err = w.wx.Commit()
			if err != nil {
				return err
			}
			err := w.save(ts)
			if err != nil {
				return err
			}
		}
		w.wx, err = w.store.shards.TrWrite(w.shard)
		if err != nil {
			return err
		}
		w.schema.Reset(w.wx)
		w.current = w.shard
	}

	err = w.schema.Write(id, e)
	if err != nil {
		return err
	}
	w.count++
	return nil
}

func (w *writer[T]) save(ts time.Time) error {
	if w.count == 0 {
		return nil
	}
	defer func() {
		w.count = 0
		w.ox.tx.Rollback()
		w.ox = nil
	}()
	err := w.ox.tx.Commit()
	if err != nil {
		return err
	}
	view := quantum.ViewByTimeUnit("", ts, 'D')
	err = w.store.shards.Update(w.current, func(tx *rbf.Tx) error {
		return w.schema.Range(func(name string, r *roaring.Bitmap) error {
			key := fmt.Sprintf("~%s;%s<", name, view)
			_, err := tx.AddRoaring(key, r)
			return err
		})
	})
	if err != nil {
		return err
	}
	return w.ox.Commit(w.current, view)
}

// Append writes data in a specific ts. Useful for migration
func (w *writer[T]) Append(ts time.Time, data []T) error {
	w.ts = ts
	for _, e := range data {
		err := w.append(e)
		if err != nil {
			return err
		}
	}
	return w.save(ts)
}

type Migration[T proto.Message] struct {
	w *writer[T]
}

func (s *Store[T]) Migrate() (*Migration[T], error) {
	w, err := s.write()
	if err != nil {
		return nil, err
	}
	return &Migration[T]{w: w}, nil
}

func (m *Migration[T]) Release() error {
	return m.w.Release()
}

func (m *Migration[T]) Append(ts time.Time, data []T) error {
	return m.w.Append(ts, data)
}
