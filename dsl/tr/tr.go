package tr

import (
	"encoding/binary"
	"fmt"

	"go.etcd.io/bbolt"
)

var (
	keys = []byte("keys")
	ids  = []byte("ids")
	seq  = []byte("seq")
)

type File struct {
	db   *bbolt.DB
	path string
}

func New(path string) *File {
	return &File{path: path}
}

func (f *File) Open() error {
	db, err := bbolt.Open(f.path, 0600, nil)
	if err != nil {
		return err
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(keys)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(ids)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(seq)
		return err
	})
	if err != nil {
		db.Close()
		return err
	}
	f.db = db
	return nil
}

func (f *File) Close() (err error) {
	if f.db != nil {
		err = f.db.Close()
		f.db = nil
	}
	return
}

func (f *File) Write() (*Write, error) {
	tx, err := f.db.Begin(true)
	if err != nil {
		return nil, err
	}
	return &Write{
		tx:   tx,
		keys: tx.Bucket(keys),
		ids:  tx.Bucket(ids),
		seq:  tx.Bucket(seq),
	}, nil
}

type Write struct {
	tx   *bbolt.Tx
	keys *bbolt.Bucket
	ids  *bbolt.Bucket
	seq  *bbolt.Bucket
}

func (w *Write) Release() error {
	return w.tx.Rollback()
}

func (w *Write) Commit() error {
	return w.tx.Commit()
}

func (w *Write) Tr(field string, key []byte) uint64 {
	next, err := w.tr(field, key)
	if err != nil {
		panic(err)
	}
	return next
}

func (w *Write) tr(field string, key []byte) (uint64, error) {
	full := append([]byte(field), key...)
	value := w.keys.Get(full)
	if value != nil {
		return binary.BigEndian.Uint64(value), nil
	}
	next, err := w.seq.NextSequence()
	if err != nil {
		return 0, fmt.Errorf("ebf/tr: getting seq id %w", err)
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], next)

	err = w.keys.Put(full, b[:])
	if err != nil {
		return 0, fmt.Errorf("ebf/tr: writing key %w", err)
	}
	fullID := append([]byte(field), b[:]...)

	err = w.ids.Put(fullID, key)
	if err != nil {
		return 0, fmt.Errorf("ebf/tr: writing key %w", err)
	}
	return next, nil
}

func (f *File) Read() (*Read, error) {
	tx, err := f.db.Begin(false)
	if err != nil {
		return nil, err
	}
	return &Read{
		tx:   tx,
		keys: tx.Bucket(keys),
		ids:  tx.Bucket(ids),
	}, nil
}

type Read struct {
	tx   *bbolt.Tx
	keys *bbolt.Bucket
	ids  *bbolt.Bucket
}

func (r *Read) Release() error {
	return r.tx.Rollback()
}

func (r *Read) Key(field string, id uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], id)
	fullID := append([]byte(field), b[:]...)
	return r.ids.Get(fullID)
}

func (r *Read) Find(field string, key []byte) (uint64, bool) {
	full := append([]byte(field), key...)
	value := r.keys.Get(full)
	if value != nil {
		return binary.BigEndian.Uint64(value), true
	}
	return 0, false
}