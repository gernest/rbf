package dsl

import (
	"fmt"
	"math"
	"time"

	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/boolean"
	"github.com/gernest/rbf/dsl/bsi"
	"github.com/gernest/rbf/dsl/mutex"
	"github.com/gernest/rbf/dsl/tr"
	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

const (
	StandardView = "standard"
	Quantum      = "20060102"
)

type Fields map[string]*roaring.Bitmap

func (f Fields) get(id string) *roaring.Bitmap {
	a, ok := f[id]
	if !ok {
		a = roaring.NewBitmap()
		f[id] = a
	}
	return a
}

type Views map[string]Fields

func (v Views) get(id string) Fields {
	a, ok := v[id]
	if !ok {
		a = make(Fields)
		v[id] = a
	}
	return a
}

type Shards map[uint64]Views

type TimeRange map[string]*roaring.Bitmap

func (t TimeRange) Set(view string, shard uint64) {
	r, ok := t[view]
	if !ok {
		r = roaring.NewBitmap(shard)
		t[view] = r
		return
	}
	if !r.Contains(shard) {
		r.DirectAdd(shard)
	}
}

func (s Shards) get(id uint64) Views {
	a, ok := s[id]
	if !ok {
		a = make(Views)
		s[id] = a
	}
	return a
}

type Writers map[string]batchWriterFunc

// Schema maps proto fields to rbf types.
type Schema[T proto.Message] struct {
	store      *Store[T]
	ops        *writeOps
	shards     Shards
	writers    Writers
	views      TimeRange
	allShards  *roaring.Bitmap
	fields     protoreflect.FieldDescriptors
	timeFormat func(value protoreflect.Value) time.Time
}

func (s *Store[T]) Schema() (*Schema[T], error) {
	var a T
	st := &Schema[T]{
		store:      s,
		timeFormat: Millisecond,
		shards:     make(Shards),
		writers:    make(Writers),
		views:      make(TimeRange),
		allShards:  roaring.NewBitmap(),
		fields:     a.ProtoReflect().Descriptor().Fields(),
	}
	return st, st.setup(a)
}

func (s *Schema[T]) Commit() error {
	return s.ops.Commit(s.allShards, s.views)
}

func (s *Schema[T]) Release() (err error) {
	if x := s.ops.Release(); x != nil {
		err = x
	}
	clear(s.shards)
	clear(s.views)
	s.allShards = roaring.NewBitmap()
	s.ops = nil
	return
}

func (s *Schema[T]) next() (uint64, error) {
	if s.ops != nil {
		return s.ops.NextID()
	}
	var err error
	s.ops, err = s.store.ops.write()
	if err != nil {
		return 0, err
	}
	return s.ops.NextID()
}

func Millisecond(value protoreflect.Value) time.Time {
	return time.UnixMilli(value.Int())
}

func Nanosecond(value protoreflect.Value) time.Time {
	return time.Unix(0, value.Int())
}

func (s *Schema[T]) TimeFormat(f func(value protoreflect.Value) time.Time) {
	s.timeFormat = f
}

type RangeCallback func(shard uint64, views Views) error

func (s *Schema[T]) Range(f RangeCallback) error {
	for k, v := range s.shards {
		err := f(k, v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Schema[T]) Write(msg T) error {
	id, err := s.next()
	if err != nil {
		return err
	}
	return s.write(id, msg.ProtoReflect())
}

const TimestampField = protoreflect.Name("timestamp")

func (s *Schema[T]) write(id uint64, msg protoreflect.Message) (err error) {
	shard := id / shardwidth.ShardWidth
	tsField := msg.Descriptor().Fields().ByName(TimestampField)
	if tsField != nil {
		view := s.timeFormat(msg.Get(tsField)).Format(Quantum)
		s.views.Set(view, shard)
	}
	if !s.allShards.Contains(shard) {
		s.allShards.DirectAdd(shard)
	}
	fields := s.shards.get(shard).get(StandardView)
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		name := string(fd.Name())
		err = s.writers[name](fields.get(name), s.ops.tr, name, id, v)
		return err == nil
	})
	return
}

type batchWriterFunc func(r *roaring.Bitmap, tr *tr.Write, field string, id uint64, value protoreflect.Value) error

func (s *Schema[T]) setup(msg proto.Message) error {
	fields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		var fn batchWriterFunc
		switch f.Kind() {
		case protoreflect.BoolKind:
			fn = func(r *roaring.Bitmap, _ *tr.Write, _ string, id uint64, value protoreflect.Value) error {
				boolean.Add(r, id, value.Bool())
				return nil
			}
		case protoreflect.EnumKind:
			fn = func(r *roaring.Bitmap, _ *tr.Write, _ string, id uint64, value protoreflect.Value) error {
				mutex.Add(r, id, uint64(value.Enum()))
				return nil
			}
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
			protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			fn = func(r *roaring.Bitmap, _ *tr.Write, _ string, id uint64, value protoreflect.Value) error {
				bsi.Add(r, id, value.Int())
				return nil
			}
		case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
			protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			fn = func(r *roaring.Bitmap, _ *tr.Write, _ string, id uint64, value protoreflect.Value) error {
				bsi.Add(r, id, int64(value.Uint()))
				return nil
			}
		case protoreflect.DoubleKind, protoreflect.FloatKind:
			fn = func(r *roaring.Bitmap, _ *tr.Write, _ string, id uint64, value protoreflect.Value) error {
				bsi.Add(r, id, int64(math.Float64bits(value.Float())))
				return nil
			}
		case protoreflect.StringKind:
			fn = func(r *roaring.Bitmap, tr *tr.Write, field string, id uint64, value protoreflect.Value) error {
				mutex.Add(r, id, tr.Tr(field, []byte(value.String())))
				return nil
			}
		case protoreflect.BytesKind:
			fn = func(r *roaring.Bitmap, tr *tr.Write, field string, id uint64, value protoreflect.Value) error {
				mutex.Add(r, id, tr.Blob(field, value.Bytes()))
				return nil
			}
		default:
			return fmt.Errorf("%s is not supported", f.Kind())
		}
		s.writers[string(f.Name())] = fn
	}
	return nil
}

func (s *Schema[T]) Save() error {
	defer s.Release()
	err := s.Range(func(shard uint64, views Views) error {
		return s.store.shards.Update(shard, func(tx *rbf.Tx) error {
			for view, fields := range views {
				for field, data := range fields {
					key := ViewKey(field, view)
					_, err := tx.AddRoaring(key, data)
					if err != nil {
						return fmt.Errorf("adding batch data for %s shard:%d %w", key, shard, err)
					}
				}
			}
			return nil
		})
	})
	if err != nil {
		return err
	}
	return s.Commit()
}
