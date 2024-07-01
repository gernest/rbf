package db

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/tr"
)

type Shards struct {
	placement sync.Map
	shards    sync.Map
	retention time.Duration
	interval  time.Duration
	log       *slog.Logger
	path      string
	mu        sync.Mutex
}

func New(path string) *Shards {
	return &Shards{
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

func (s *Shards) TrWrite(shard uint64) (*tr.Write, error) {
	db, err := s.get(shard)
	if err != nil {
		return nil, err
	}
	return db.tr.Write()
}

func (s *Shards) TrRead(shard uint64) (*tr.Read, error) {
	db, err := s.get(shard)
	if err != nil {
		return nil, err
	}
	return db.tr.Read()
}

func (s *Shards) tx(shard uint64, update bool, f func(tx *rbf.Tx) error) error {
	db, err := s.get(shard)
	if err != nil {
		return err
	}
	tx, err := db.db.Begin(update)
	if err != nil {
		return err
	}
	err = f(tx)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Shards) get(shard uint64) (*data, error) {
	g, ok := s.shards.Load(shard)
	if ok {
		db := g.(*data)

		if !db.IsOpen() {
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
	db := newData(s.path, shard)
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

type data struct {
	db   *rbf.DB
	tr   *tr.File
	open atomic.Bool
}

func newData(path string, shard uint64) *data {
	fullDB := filepath.Join(path, "rbf", fmt.Sprintf("%06d", shard))
	fullTR := filepath.Join(fullDB, "TRANSLATE")
	return &data{
		db: rbf.NewDB(fullDB, nil),
		tr: tr.New(fullTR),
	}
}

func (d *data) IsOpen() bool {
	return d.open.Load()
}

func (d *data) Open() error {
	if d.IsOpen() {
		return nil
	}
	err := d.db.Open()
	if err != nil {
		return err
	}
	err = d.tr.Open()
	if err != nil {
		d.db.Close()
		return err
	}
	d.open.Store(true)
	return nil
}

func (d *data) Close() error {
	if !d.IsOpen() {
		return nil
	}
	defer d.open.Store(false)
	return errors.Join(d.db.Close(), d.tr.Close())
}
