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
	"github.com/gernest/rbf/quantum"
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

func (s Shards) get(id uint64) Views {
	a, ok := s[id]
	if !ok {
		a = make(Views)
		s[id] = a
	}
	return a
}

type Writers map[string]writeFn

// Schema maps proto fields to rbf types.
type Schema[T proto.Message] struct {
	store          *Store[T]
	ops            *writeOps
	shards         Shards
	writers        Writers
	fields         protoreflect.FieldDescriptors
	timeFormat     func(value protoreflect.Value) time.Time
	timestampField protoreflect.Name
	views          []string
}

func (s *Store[T]) Schema() (*Schema[T], error) {
	var a T
	st := &Schema[T]{
		store:          s,
		timeFormat:     Millisecond,
		shards:         make(Shards),
		writers:        make(Writers),
		fields:         a.ProtoReflect().Descriptor().Fields(),
		timestampField: "timestamp",
	}
	return st, st.setup(a)
}

func (s *Schema[T]) Commit(m map[string]*roaring.Bitmap) error {
	return s.ops.Commit(m)
}

func (s *Schema[T]) Release() (err error) {
	if x := s.ops.Release(); x != nil {
		err = x
	}
	clear(s.shards)
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
	return time.Unix(0, int64(value.Uint()))
}

func (s *Schema[T]) TimeFormat(field string, f func(value protoreflect.Value) time.Time) {
	s.timeFormat = f
	s.timestampField = protoreflect.Name(field)
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

func (s *Schema[T]) write(id uint64, msg protoreflect.Message) (err error) {
	shard := id / shardwidth.ShardWidth
	tsField := msg.Descriptor().Fields().ByName(s.timestampField)
	s.views = append(s.views[:0], StandardView)
	if tsField != nil {
		ts := s.timeFormat(msg.Get(tsField))
		s.views = append(s.views, quantum.ViewByTimeUnit(StandardView, ts, 'D'))
	}
	vs := s.shards.get(shard)
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		name := string(fd.Name())
		for _, view := range s.views {
			err = s.writers[name](vs.get(view).get(name), s.ops.tr, name, id, v)
			if err != nil {
				return false
			}
		}
		return true
	})
	return
}

type writeFn func(r *roaring.Bitmap, tr *tr.Write, field string, id uint64, value protoreflect.Value) error

func (s *Schema[T]) setup(msg proto.Message) error {
	fields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		var fn writeFn
		if f.IsList() {
			// only []string  and [][]byte is supported
			switch f.Kind() {
			case protoreflect.StringKind:
				fn = func(r *roaring.Bitmap, tr *tr.Write, field string, id uint64, value protoreflect.Value) error {
					ls := value.List()
					for n := 0; n < ls.Len(); n++ {
						mutex.Add(r, id, tr.Tr(field, []byte(ls.Get(n).String())))
					}
					return nil
				}
			case protoreflect.BytesKind:
				fn = func(r *roaring.Bitmap, tr *tr.Write, field string, id uint64, value protoreflect.Value) error {
					ls := value.List()
					for n := 0; n < ls.Len(); n++ {
						mutex.Add(r, id, tr.Blob(field, ls.Get(n).Bytes()))
					}
					return nil
				}
			default:
				return fmt.Errorf("%s is not supported", f.Kind())
			}
			s.writers[string(f.Name())] = fn
			continue
		}
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
		case protoreflect.Int64Kind:
			fn = func(r *roaring.Bitmap, _ *tr.Write, _ string, id uint64, value protoreflect.Value) error {
				bsi.Add(r, id, value.Int())
				return nil
			}
		case protoreflect.Uint64Kind:
			fn = func(r *roaring.Bitmap, _ *tr.Write, _ string, id uint64, value protoreflect.Value) error {
				bsi.Add(r, id, int64(value.Uint()))
				return nil
			}
		case protoreflect.DoubleKind:
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
	m := map[string]*roaring.Bitmap{}
	err := s.Range(func(shard uint64, views Views) error {
		return s.store.shards.Update(shard, func(tx *rbf.Tx) error {
			for view, fields := range views {
				b, ok := m[view]
				if !ok {
					b = roaring.NewBitmap(shard)
					m[view] = b
				} else {
					if !b.Contains(shard) {
						b.DirectAdd(shard)
					}
				}
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
	return s.Commit(m)
}
