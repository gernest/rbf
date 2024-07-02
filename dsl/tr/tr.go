package tr

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/blevesearch/vellum"
	"github.com/cespare/xxhash/v2"
	"go.etcd.io/bbolt"
)

var (
	keys     = []byte("keys")
	ids      = []byte("ids")
	seq      = []byte("seq")
	blobHash = []byte("hash")
	fst      = []byte("fst")
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
		_, err = tx.CreateBucketIfNotExists(blobHash)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(fst)
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
		tx:      tx,
		keys:    tx.Bucket(keys),
		ids:     tx.Bucket(ids),
		blobs:   tx.Bucket(blobHash),
		seq:     tx.Bucket(seq),
		fst:     tx.Bucket(fst),
		touched: make(map[string]struct{}),
	}, nil
}

type Write struct {
	tx    *bbolt.Tx
	keys  *bbolt.Bucket
	ids   *bbolt.Bucket
	blobs *bbolt.Bucket
	seq   *bbolt.Bucket
	fst   *bbolt.Bucket

	// tracks updated fields. Helps to avoid building fst for fields that were
	// never updated.
	touched map[string]struct{}
}

func (w *Write) Release() error {
	return w.tx.Rollback()
}

func (w *Write) Commit() error {
	err := w.vellum()
	if err != nil {
		return err
	}
	return w.tx.Commit()
}

func (w *Write) vellum() error {
	var o bytes.Buffer
	b, err := vellum.New(&o, nil)
	if err != nil {
		return err
	}
	return w.keys.ForEachBucket(func(k []byte) error {
		if _, ok := w.touched[string(k)]; !ok {
			// Avoid rebuilding fst for fields that were never updated.
			return nil
		}
		o.Reset()
		err := b.Reset(&o)
		if err != nil {
			return err
		}
		err = w.keys.Bucket(k).ForEach(func(k, v []byte) error {
			return b.Insert(k, binary.BigEndian.Uint64(v))
		})
		if err != nil {
			return err
		}
		err = b.Close()
		if err != nil {
			return err
		}
		return w.fst.Put(k, bytes.Clone(o.Bytes()))
	})
}

func (w *Write) Blob(field string, data []byte) uint64 {
	next, err := w.blob(field, data)
	if err != nil {
		panic(err)
	}
	return next
}

func (w *Write) Tr(field string, key []byte) uint64 {
	next, err := w.tr(field, key)
	if err != nil {
		panic(err)
	}
	return next
}

func (w *Write) tr(field string, key []byte) (uint64, error) {
	keys, exists, err := w.bucket(w.keys, []byte(field))
	if err != nil {
		return 0, err
	}
	if exists {
		// fast path: hey already translated.
		if value := keys.Get(key); value != nil {
			return binary.BigEndian.Uint64(value), nil
		}
	}
	next, err := w.seq.NextSequence()
	if err != nil {
		return 0, fmt.Errorf("ebf/tr: getting seq id %w", err)
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], next)

	err = keys.Put(key, b[:])
	if err != nil {
		return 0, fmt.Errorf("ebf/tr: writing key %w", err)
	}

	fullID := append([]byte(field), b[:]...)

	err = w.ids.Put(fullID, key)
	if err != nil {
		return 0, fmt.Errorf("ebf/tr: writing key %w", err)
	}
	w.touched[field] = struct{}{}
	return next, nil
}

func (w *Write) bucket(b *bbolt.Bucket, key []byte) (*bbolt.Bucket, bool, error) {
	g := b.Bucket(key)
	if g != nil {
		return g, true, nil
	}
	g, err := b.CreateBucket(key)
	return g, false, err
}

func (w *Write) blob(field string, data []byte) (uint64, error) {
	hash := xxhash.Sum64(data)
	var id [8]byte
	binary.BigEndian.PutUint64(id[:], hash)
	full := append([]byte(field), id[:]...)
	value := w.blobs.Get(full)
	if value != nil {
		return hash, nil
	}
	err := w.blobs.Put(full, data)
	if err != nil {
		return 0, fmt.Errorf("ebf/tr: writing blob data %w", err)
	}
	return hash, nil
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
		fst:  tx.Bucket(fst),
		blob: tx.Bucket(blobHash),
	}, nil
}

type Read struct {
	tx   *bbolt.Tx
	keys *bbolt.Bucket
	ids  *bbolt.Bucket
	fst  *bbolt.Bucket
	blob *bbolt.Bucket
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
	keys := r.keys.Bucket([]byte(field))
	if keys == nil {
		return 0, false
	}
	value := keys.Get(key)
	if value != nil {
		return binary.BigEndian.Uint64(value), true
	}
	return 0, false
}

// Blob returns data stored for blob id.
func (r *Read) Blob(field string, id uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], id)
	full := append([]byte(field), b[:]...)
	return r.blob.Get(full)
}

func (r *Read) Search(field string, a vellum.Automaton, start, end []byte) ([]uint64, error) {
	b := r.fst.Get([]byte(field))
	if b == nil {
		return []uint64{}, nil
	}
	fst, err := vellum.Load(b)
	if err != nil {
		return nil, err
	}
	var result []uint64
	it, err := fst.Search(a, start, end)
	for err == nil {
		_, value := it.Current()
		result = append(result, value)
		err = it.Next()
	}
	return result, nil
}
