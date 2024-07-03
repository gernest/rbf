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
	blobHash = []byte("blob_hash")
	blobID   = []byte("blob_id")
	fst      = []byte("fst")
)

var emptyKey = []byte{
	0x00, 0x00, 0x00,
	0x4d, 0x54, 0x4d, 0x54, // MTMT
	0x00,
	0xc2, 0xa0, // NO-BREAK SPACE
	0x00,
}

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
		_, err = tx.CreateBucketIfNotExists(blobID)
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(fst)
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
		tx:       tx,
		keys:     tx.Bucket(keys),
		ids:      tx.Bucket(ids),
		blobID:   tx.Bucket(blobID),
		blobHash: tx.Bucket(blobHash),
		fst:      tx.Bucket(fst),
		touched:  make(map[string]struct{}),
	}, nil
}

type Write struct {
	tx       *bbolt.Tx
	keys     *bbolt.Bucket
	ids      *bbolt.Bucket
	blobID   *bbolt.Bucket
	blobHash *bbolt.Bucket
	fst      *bbolt.Bucket

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
			if bytes.Equal(k, emptyKey) {
				// skip empty keys
				return nil
			}
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
	if len(key) == 0 {
		key = emptyKey
	}
	keys, err := bucket(w.keys, []byte(field))
	if err != nil {
		return 0, fmt.Errorf("ebf/tr: setup keys bucket %w", err)
	}
	// fast path: hey already translated.
	if value := keys.Get(key); value != nil {
		return binary.BigEndian.Uint64(value), nil
	}
	ids, err := bucket(w.ids, []byte(field))
	if err != nil {
		return 0, fmt.Errorf("ebf/tr: setup ids bucket %w", err)
	}
	next, err := ids.NextSequence()
	if err != nil {
		return 0, fmt.Errorf("ebf/tr: getting seq id %w", err)
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], next)

	err = keys.Put(key, b[:])
	if err != nil {
		return 0, fmt.Errorf("ebf/tr: writing key %w", err)
	}
	err = ids.Put(b[:], key)
	if err != nil {
		return 0, fmt.Errorf("ebf/tr: writing key %w", err)
	}
	w.touched[field] = struct{}{}
	return next, nil
}

func (w *Write) blob(field string, key []byte) (uint64, error) {
	if len(key) == 0 {
		key = emptyKey
	}
	keys, err := bucket(w.blobHash, []byte(field))
	if err != nil {
		return 0, fmt.Errorf("ebf/tr: setup blob keys bucket %w", err)
	}
	hash := xxhash.Sum64(key)

	var hk [8]byte
	binary.BigEndian.PutUint64(hk[:], hash)

	// fast path: hey already translated.
	if value := keys.Get(hk[:]); value != nil {
		return binary.BigEndian.Uint64(value), nil
	}
	ids, err := bucket(w.blobID, []byte(field))
	if err != nil {
		return 0, fmt.Errorf("ebf/tr: setup blob ids bucket %w", err)
	}
	next, err := ids.NextSequence()
	if err != nil {
		return 0, fmt.Errorf("ebf/tr: getting seq id %w", err)
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], next)

	err = keys.Put(hk[:], b[:])
	if err != nil {
		return 0, fmt.Errorf("ebf/tr: writing blob key %w", err)
	}
	err = ids.Put(b[:], key)
	if err != nil {
		return 0, fmt.Errorf("ebf/tr: writing blob id %w", err)
	}
	w.touched[field] = struct{}{}
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
		fst:  tx.Bucket(fst),
		blob: tx.Bucket(blobID),
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
	ids := r.ids.Bucket([]byte(field))
	if ids == nil {
		return nil
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], id)
	return get(ids.Get(b[:]))
}

func get(key []byte) []byte {
	if bytes.Equal(key, emptyKey) {
		return []byte{}
	}
	return key
}

func (r *Read) Find(field string, key []byte) (uint64, bool) {
	if len(key) == 0 {
		key = emptyKey
	}
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
	ids := r.blob.Bucket([]byte(field))
	if ids == nil {
		return nil
	}
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], id)
	return get(ids.Get(b[:]))
}

func (r *Read) Search(field string, a vellum.Automaton, start, end []byte, match func(key []byte, value uint64)) error {
	b := r.fst.Get([]byte(field))
	if b == nil {
		return nil
	}
	fst, err := vellum.Load(b)
	if err != nil {
		return err
	}
	it, err := fst.Search(a, start, end)
	for err == nil {
		match(it.Current())
		err = it.Next()
	}
	return nil
}

func bucket(b *bbolt.Bucket, key []byte) (*bbolt.Bucket, error) {
	if v := b.Bucket(key); v != nil {
		return v, nil
	}
	return b.CreateBucket(key)
}
