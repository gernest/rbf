package dsl

import (
	"encoding/binary"
	"path/filepath"

	"go.etcd.io/bbolt"
)

var (
	viewsBucket = []byte("views")
	seqBucket   = []byte("seq")
)

type Ops struct {
	db *bbolt.DB
}

func newOps(path string) (*Ops, error) {
	full := filepath.Join(path, "OPS")
	db, err := bbolt.Open(full, 0600, nil)
	if err != nil {
		return nil, err
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(viewsBucket)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(seqBucket)
		return err
	})
	if err != nil {
		db.Close()
		return nil, err
	}
	return &Ops{db: db}, nil
}

func (o *Ops) write() (*writeOps, error) {
	tx, err := o.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &writeOps{
		tx:    tx,
		views: tx.Bucket(viewsBucket),
		seq:   tx.Bucket(seqBucket),
	}, nil
}

type writeOps struct {
	tx    *bbolt.Tx
	views *bbolt.Bucket
	seq   *bbolt.Bucket
}

func (o *writeOps) NextID() (uint64, error) {
	return o.seq.NextSequence()
}

func (o *writeOps) Release() error {
	if o == nil {
		return nil
	}
	return o.tx.Rollback()
}

func (o *writeOps) Commit(m map[string][]uint64) error {
	defer o.tx.Rollback()

	for view, shards := range m {
		vb, err := o.views.CreateBucketIfNotExists([]byte(view))
		if err != nil {
			return err
		}
		for _, shard := range shards {
			var b [8]byte
			binary.BigEndian.PutUint64(b[:], shard)
			err = vb.Put(b[:], []byte{})
			if err != nil {
				return err
			}
		}
	}
	return o.tx.Commit()
}
