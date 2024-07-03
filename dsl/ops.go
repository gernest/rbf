package dsl

import (
	"bytes"
	"fmt"
	"path/filepath"

	"github.com/gernest/roaring"
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

func (o *Ops) read() (*readOps, error) {
	tx, err := o.db.Begin(false)
	if err != nil {
		return nil, err
	}
	return &readOps{
		tx:    tx,
		views: tx.Bucket(viewsBucket),
	}, nil
}

type readOps struct {
	tx    *bbolt.Tx
	views *bbolt.Bucket
}

func (r *readOps) Release() error {
	return r.tx.Rollback()
}

func (r *readOps) All() (o []uint64) {
	data := r.views.Get([]byte{0})
	if data != nil {
		r := roaring.NewBitmap()
		r.UnmarshalBinary(data)
		o = r.Slice()
	}
	return
}

// Shards returns all shards for the given view.
func (r *readOps) Shards(view string) (o []uint64) {
	data := r.views.Get([]byte(view))
	if data != nil {
		r := roaring.NewBitmap()
		r.UnmarshalBinary(data)
		o = r.Slice()
	}
	return
}

// ShardsRange returns all shards found in range. Range is inclusive.
func (r *readOps) ShardsRange(from, to string) (o []uint64) {
	c := r.views.Cursor()
	b := roaring.NewBitmap()
	last := []byte(to)
	for k, v := c.Seek([]byte(from)); bytes.Compare(k, last) <= 0; k, v = c.Next() {
		r := roaring.NewBitmap()
		r.UnmarshalBinary(v)
		b.UnionInPlace(r)
	}
	o = b.Slice()
	return
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

func (o *writeOps) Commit(all *roaring.Bitmap, m map[string]*roaring.Bitmap) error {
	defer o.tx.Rollback()

	for view, shards := range m {
		if data := o.views.Get([]byte(view)); data != nil {
			r := roaring.NewBitmap()
			err := r.UnmarshalBinary(data)
			if err != nil {
				return fmt.Errorf("reading shards bitmap %w", err)
			}
			shards.IntersectInPlace(r)
		}
		data, err := shards.MarshalBinary()
		if err != nil {
			return fmt.Errorf("marshal shards bitmap %w", err)
		}
		err = o.views.Put([]byte(view), data)
		if err != nil {
			return fmt.Errorf("put shards bitmap %w", err)
		}
	}
	if data := o.views.Get([]byte{0}); data != nil {
		r := roaring.NewBitmap()
		err := r.UnmarshalBinary(data)
		if err != nil {
			return fmt.Errorf("reading shards bitmap %w", err)
		}
		all.IntersectInPlace(r)
	}
	data, err := all.MarshalBinary()
	if err != nil {
		return fmt.Errorf("marshal all shards bitmap %w", err)
	}
	err = o.views.Put([]byte{0}, data)
	if err != nil {
		return fmt.Errorf("put all shards bitmap %w", err)
	}
	return o.tx.Commit()
}
