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

func (o *Ops) ops() (*ops, error) {
	tx, err := o.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &ops{
		tx:    tx,
		views: tx.Bucket(viewsBucket),
		seq:   tx.Bucket(seqBucket),
	}, nil
}

type ops struct {
	tx    *bbolt.Tx
	views *bbolt.Bucket
	seq   *bbolt.Bucket
}

func (o *ops) NextID() (uint64, error) {
	return o.seq.NextSequence()
}

func (o *ops) Commit(shard uint64, view string) error {
	defer o.tx.Rollback()
	vb, err := o.views.CreateBucketIfNotExists([]byte(view))
	if err != nil {
		return err
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], shard)
	err = vb.Put(b[:], []byte{})
	if err != nil {
		return err
	}
	return o.tx.Commit()
}
