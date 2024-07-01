package schema

import (
	"fmt"
	"math"

	"github.com/gernest/rbf/dsl/boolean"
	"github.com/gernest/rbf/dsl/bsi"
	"github.com/gernest/rbf/dsl/mutex"
	"github.com/gernest/rbf/dsl/tr"
	"github.com/gernest/roaring"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// Schema maps proto fields to rbf types.
type Schema[T proto.Message] struct {
	batch   map[protoreflect.FieldNumber]*roaring.Bitmap
	writers map[protoreflect.FieldNumber]batchWriterFunc
	msg     protoreflect.MessageDescriptor
	tr      *tr.Write
}

func NewSchema[T proto.Message](tr *tr.Write) (*Schema[T], error) {
	var a T
	s := &Schema[T]{tr: tr, msg: a.ProtoReflect().Descriptor(),
		batch:   make(map[protowire.Number]*roaring.Bitmap),
		writers: make(map[protowire.Number]batchWriterFunc)}
	return s, s.setup(a)
}

func (s *Schema[T]) Reset(tr *tr.Write) {
	clear(s.batch)
	s.tr = tr
}

func (s *Schema[T]) Range(f func(name string, r *roaring.Bitmap) error) error {
	fs := s.msg.Fields()
	for k, v := range s.batch {
		name := fs.ByNumber(k).Name()
		err := f(string(name), v)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Schema[T]) Write(id uint64, msg T) error {
	return s.write(id, msg.ProtoReflect())
}

func (s *Schema[T]) write(id uint64, msg protoreflect.Message) (err error) {
	msg.Range(func(fd protoreflect.FieldDescriptor, v protoreflect.Value) bool {
		b, ok := s.batch[fd.Number()]
		if !ok {
			b = roaring.NewBitmap()
			s.batch[fd.Number()] = b
		}
		err = s.writers[fd.Number()](b, id, v)
		return err == nil
	})
	return
}

type batchWriterFunc func(r *roaring.Bitmap, id uint64, value protoreflect.Value) error

func (s *Schema[T]) setup(msg proto.Message) error {
	fields := msg.ProtoReflect().Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		f := fields.Get(i)
		var fn batchWriterFunc
		switch f.Kind() {
		case protoreflect.BoolKind:
			fn = func(r *roaring.Bitmap, id uint64, value protoreflect.Value) error {
				boolean.Add(r, id, value.Bool())
				return nil
			}
		case protoreflect.EnumKind:
			fn = func(r *roaring.Bitmap, id uint64, value protoreflect.Value) error {
				mutex.Add(r, id, uint64(value.Enum()))
				return nil
			}
		case protoreflect.Int32Kind, protoreflect.Sint32Kind, protoreflect.Sfixed32Kind,
			protoreflect.Int64Kind, protoreflect.Sint64Kind, protoreflect.Sfixed64Kind:
			fn = func(r *roaring.Bitmap, id uint64, value protoreflect.Value) error {
				bsi.Add(r, id, value.Int())
				return nil
			}
		case protoreflect.Uint32Kind, protoreflect.Fixed32Kind,
			protoreflect.Uint64Kind, protoreflect.Fixed64Kind:
			fn = func(r *roaring.Bitmap, id uint64, value protoreflect.Value) error {
				bsi.Add(r, id, int64(value.Uint()))
				return nil
			}
		case protoreflect.DoubleKind, protoreflect.FloatKind:
			fn = func(r *roaring.Bitmap, id uint64, value protoreflect.Value) error {
				bsi.Add(r, id, int64(math.Float64bits(value.Float())))
				return nil
			}
		case protoreflect.StringKind:
			fn = s.str(string(f.Name()))
		case protoreflect.BytesKind:
			fn = s.bytes(string(f.Name()))
		default:
			return fmt.Errorf("%s is not supported", f.Kind())
		}
		s.writers[f.Number()] = fn
	}
	return nil
}

func (s *Schema[T]) str(n string) batchWriterFunc {
	return func(r *roaring.Bitmap, id uint64, value protoreflect.Value) error {
		bsi.Add(r, id, int64(s.tr.Tr(n, []byte(value.String()))))
		return nil
	}
}

func (s *Schema[T]) bytes(n string) batchWriterFunc {
	return func(r *roaring.Bitmap, id uint64, value protoreflect.Value) error {
		bsi.Add(r, id, int64(s.tr.Blob(n, value.Bytes())))
		return nil
	}
}
