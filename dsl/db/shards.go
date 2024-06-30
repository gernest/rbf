package db

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/gernest/rbf"
	"github.com/gernest/rbf/cfg"
)

type Shards struct {
	placement sync.Map
	shards    sync.Map
	config    cfg.Config
	retention time.Duration
	interval  time.Duration
	log       *slog.Logger
	path      string
	mu        sync.Mutex
}

func New(path string) *Shards {
	config := cfg.NewDefaultConfig()
	return &Shards{
		config:    *config,
		retention: 24 * time.Hour,
		interval:  time.Minute,
		log:       slog.Default().With("component", "rbf/db"),
		path:      filepath.Join(path, "rbf"),
	}
}

func (s *Shards) Start(ctx context.Context) {
	ts := time.NewTicker(s.interval)
	defer ts.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ts.C:
			s.placement.Range(func(key, value any) bool {
				a := value.(*placement)
				if a.expires.Before(now) {
					err := a.db.Close()
					if err != nil {
						s.log.Error("closing expired shard", "path", a.db.Path, "err", err)
					}
					s.placement.Delete(key.(uint64))
				}
				return true
			})
		}
	}
}

func (s *Shards) Close() (err error) {
	s.shards.Range(func(key, value any) bool {
		db := value.(*rbf.DB)
		if !db.IsClosed() {
			x := db.Close()
			if x != nil {
				err = x
			}
		}
		s.shards.Delete(key)
		s.placement.Delete(key)
		return true
	})
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
	err = f(tx)
	if err != nil {
		tx.Rollback()
		return err
	}
	if update {
		return tx.Commit()
	}
	tx.Rollback()
	return nil
}

func (s *Shards) get(shard uint64) (*rbf.DB, error) {
	g, ok := s.shards.Load(shard)
	if ok {
		db := g.(*rbf.DB)
		if db.IsClosed() {
			err := db.Open()
			if err != nil {
				return nil, fmt.Errorf("rbf/db: reopening closed shard %w", err)
			}
			// create new placement
			s.placement.Store(shard, &placement{
				expires: time.Now().Add(s.retention),
			})
		}
		return db, nil
	}
	// Guarantee only one thread gets to create anew shard
	s.mu.Lock()
	config := s.config
	path := filepath.Join(s.path, fmt.Sprintf("%06d", shard))
	db := rbf.NewDB(path, &config)
	err := db.Open()
	if err != nil {
		s.mu.Unlock()
		return nil, fmt.Errorf("rbf/db: opening shard %w", err)
	}
	s.shards.Store(shard, db)
	s.placement.Store(shard, &placement{
		expires: time.Now().Add(s.retention),
	})
	s.mu.Unlock()
	return db, nil
}

type placement struct {
	expires time.Time
	db      *rbf.DB
}
