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
)

func (s *Store[T]) process(ctx context.Context, data <-chan T) error {
	schema, err := schema.NewSchema[T](nil)
	if err != nil {
		return err
	}
	var (
		ox    *ops
		wx    *tr.Write
		shard uint64
		count uint64
	)
	begin := true

	currentShard := ^uint64(0)
	next := func() (uint64, error) {
		if ox != nil {
			return ox.NextID()
		}
		ox, err = s.ops.ops()
		if err != nil {
			return 0, err
		}
		nxt, err := ox.NextID()
		if err != nil {
			return 0, err
		}
		shard = nxt / shardwidth.ShardWidth
		return nxt, nil
	}
	save := func(ts time.Time) error {
		if count == 0 {
			return nil
		}
		defer func() {
			count = 0
			ox.tx.Rollback()
			ox = nil
		}()
		err = ox.tx.Commit()
		if err != nil {
			return err
		}
		view := quantum.ViewByTimeUnit("", ts, 'D')
		err = s.shards.Update(currentShard, func(tx *rbf.Tx) error {
			return schema.Range(func(name string, r *roaring.Bitmap) error {
				key := fmt.Sprintf("~%s;%s<", name, view)
				_, err := tx.AddRoaring(key, r)
				return err
			})
		})
		if err != nil {
			return err
		}
		return ox.Commit(currentShard, view)
	}

	tick := time.NewTicker(time.Minute)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-data:
			id, err := next()
			if err != nil {
				return err
			}
			if currentShard != shard {
				if !begin {
					// We have changed shards, save the last shard and reset state. Saving
					// views is per time observed by the server.
					//
					// We do not assume T has timestamp
					err := save(time.Now())
					if err != nil {
						return err
					}
					err = wx.Commit()
					if err != nil {
						return err
					}
				} else {
					begin = false
				}
				wx, err = s.shards.TrWrite(shard)
				if err != nil {
					return err
				}
				schema.Reset(wx)
				currentShard = shard
			}
			err = schema.Write(id, e)
			if err != nil {
				return err
			}
			count++
		case ts := <-tick.C:
			err = save(ts)
			if err != nil {
				return err
			}
		}
	}
}
