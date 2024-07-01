package dsl

import (
	"encoding/binary"
	"time"

	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/tr"
	"github.com/gernest/rbf/quantum"
)

type Tx struct {
	Tx    *rbf.Tx
	Shard uint64
	Tr    *tr.Read
}

func (s *Store[T]) View(start, end time.Time, f func(tx *Tx) error) error {
	var views []string
	if date(start).Equal(date(end)) {
		views = []string{quantum.ViewByTimeUnit("", start, 'D')}
	} else {
		views = quantum.ViewsByTimeRange("", start, end, "D")
	}
	otx, err := s.ops.db.Begin(false)
	if err != nil {
		return err
	}
	defer otx.Rollback()
	vb := otx.Bucket(viewsBucket)
	for _, view := range views {
		x := vb.Bucket([]byte(view))
		if x == nil {
			continue
		}
		cursor := x.Cursor()

		for k, _ := cursor.First(); k != nil; k, _ = cursor.Next() {
			shard := binary.BigEndian.Uint64(k)
			err := s.shards.View2(shard, func(tx *rbf.Tx, tr *tr.Read) error {
				return f(&Tx{
					Tx:    tx,
					Shard: shard,
					Tr:    tr,
				})
			})
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func date(ts time.Time) time.Time {
	y, m, d := ts.Date()
	return time.Date(y, m, d, 0, 0, 0, 0, time.UTC)
}
