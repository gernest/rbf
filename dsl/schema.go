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
		fields:         a.ProtoReflect().Descriptor().Fields(),
		timestampField: "timestamp",
	}
	return st, st.validate(a)
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
		kind := fd.Kind()
		for _, view := range s.views {
			writers[kind](vs.get(view).get(name), s.ops.tr, fd, id, v)
		}
		return true
	})
	return
}

type writeFn func(r *roaring.Bitmap, tr *tr.Write, field protoreflect.FieldDescriptor, id uint64, value protoreflect.Value)

func (s *Schema[T]) validate(msg proto.Message) error {
	fields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		if f.IsList() {
			// only []string  and [][]byte is supported
			switch f.Kind() {
			case protoreflect.StringKind,
				protoreflect.BytesKind:
			default:
				return fmt.Errorf("%s list is not supported", f.Kind())
			}
			continue
		}
		switch f.Kind() {
		case protoreflect.BoolKind,
			protoreflect.EnumKind,
			protoreflect.Int64Kind,
			protoreflect.Uint64Kind,
			protoreflect.DoubleKind,
			protoreflect.StringKind,
			protoreflect.BytesKind:
		default:
			return fmt.Errorf("%s is not supported", f.Kind())
		}
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

var (
	writers = map[protoreflect.Kind]writeFn{
		protoreflect.BoolKind:   Bool,
		protoreflect.EnumKind:   Enum,
		protoreflect.Int64Kind:  Int64,
		protoreflect.Uint64Kind: Uint64,
		protoreflect.DoubleKind: Float64,
		protoreflect.StringKind: String,
		protoreflect.BytesKind:  Bytes,
	}
)

// Bool writes a boolean proto value
func Bool(r *roaring.Bitmap, _ *tr.Write, _ protoreflect.FieldDescriptor, id uint64, value protoreflect.Value) {
	boolean.Add(r, id, value.Bool())
}

// Enum writes enum proto value
func Enum(r *roaring.Bitmap, _ *tr.Write, _ protoreflect.FieldDescriptor, id uint64, value protoreflect.Value) {
	mutex.Add(r, id, uint64(value.Enum()))
}

// Int64 writes int64 proto value
func Int64(r *roaring.Bitmap, _ *tr.Write, _ protoreflect.FieldDescriptor, id uint64, value protoreflect.Value) {
	bsi.Add(r, id, value.Int())
}

func Uint64(r *roaring.Bitmap, _ *tr.Write, _ protoreflect.FieldDescriptor, id uint64, value protoreflect.Value) {
	bsi.Add(r, id, int64(value.Uint()))
}

func Float64(r *roaring.Bitmap, _ *tr.Write, _ protoreflect.FieldDescriptor, id uint64, value protoreflect.Value) {
	bsi.Add(r, id, int64(math.Float64bits(value.Float())))
}

func String(r *roaring.Bitmap, tr *tr.Write, field protoreflect.FieldDescriptor, id uint64, value protoreflect.Value) {
	name := field.Name()
	if field.IsList() {
		ls := value.List()
		for i := 0; i < ls.Len(); i++ {
			mutex.Add(r, id, tr.Tr(string(name), []byte(ls.Get(i).String())))
		}
	} else {
		mutex.Add(r, id, tr.Tr(string(name), []byte(value.String())))
	}
}

func Bytes(r *roaring.Bitmap, tr *tr.Write, field protoreflect.FieldDescriptor, id uint64, value protoreflect.Value) {
	name := field.Name()
	if field.IsList() {
		ls := value.List()
		for i := 0; i < ls.Len(); i++ {
			mutex.Add(r, id, tr.Blob(string(name), ls.Get(i).Bytes()))
		}
	} else {
		mutex.Add(r, id, tr.Blob(string(name), value.Bytes()))
	}
}
