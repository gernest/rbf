package db

import (
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"

	"github.com/dgraph-io/ristretto"
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/cfg"
)

type Shards struct {
	cache *ristretto.Cache
	log   *slog.Logger
	path  string
	mu    sync.Mutex
}

func New(path string) (*Shards, error) {
	log := slog.Default().With(slog.String("component", "rbf/db"))
	cache, err := ristretto.NewCache(&ristretto.Config{
		NumCounters: 1e7,
		// shards are memory mapped. Maximum database size is 4 GB and we create
		// database for each shard.
		//
		// The nature of data api guarantees smaller database per shard. We allow up
		// to 4 GB of shards databases to be in memory at any single time.
		MaxCost:     4 << 30,
		BufferItems: 64, // number of keys per Get buffer.
		OnEvict: func(item *ristretto.Item) {
			err := item.Value.(*rbf.DB).Close()
			if err != nil {
				log.Error("evicting database shard", "shard", item.Key)
			}
			item.Value = nil
		},
		OnReject: func(item *ristretto.Item) {
			err := item.Value.(*rbf.DB).Close()
			if err != nil {
				log.Error("evicting database shard", "shard", item.Key)
			}
			item.Value = nil
		},
	})

	if err != nil {
		return nil, fmt.Errorf("creating database cache %w", err)
	}
	return &Shards{
		cache: cache,
		log:   log,
		path:  filepath.Join(path, "rbf"),
	}, nil
}

func (s *Shards) Close() (err error) {
	s.cache.Close()
	return
}

func (s *Shards) View(shard uint64, f func(tx *rbf.Tx) error) error {
	return s.tx(shard, false, f)
}

func (s *Shards) Update(shard uint64, f func(tx *rbf.Tx) error) error {
	return s.tx(shard, true, f)
}

func (s *Shards) tx(shard uint64, update bool, f func(tx *rbf.Tx) error) error {
	db, err := s.get(shard)
	if err != nil {
		return err
	}
	tx, err := db.Begin(update)
	if err != nil {
		return err
	}
	if !update {
		defer tx.Rollback()
		return f(tx)
	}
	err = f(tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (s *Shards) get(shard uint64) (*rbf.DB, error) {
	if db, ok := s.cache.Get(shard); ok {
		return db.(*rbf.DB), nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	o := cfg.NewDefaultConfig()
	o.Logger = s.log.With(slog.Uint64("shard", shard))
	path := s.dbPath(shard)
	db := rbf.NewDB(path, o)
	err := db.Open()
	if err != nil {
		return nil, fmt.Errorf("opening shard database at %s %w", path, err)
	}
	size, err := db.Size()
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("obtaining database size at %s %w", path, err)
	}
	// Make sure the database is accepted in the cache before returning it.
	for range 5 {
		if s.cache.Set(shard, db, size) {
			break
		}
	}
	return db, nil
}

func (s *Shards) dbPath(shard uint64) string {
	return filepath.Join(s.path, fmt.Sprintf("%06d", shard))
}
