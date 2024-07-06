// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.30.0
// 	protoc        v3.21.12
// source: msg.proto

package kase

import (
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
	protoimpl "google.golang.org/protobuf/runtime/protoimpl"
	reflect "reflect"
	sync "sync"
)

const (
	// Verify that this generated code is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(20 - protoimpl.MinVersion)
	// Verify that runtime/protoimpl is sufficiently up-to-date.
	_ = protoimpl.EnforceVersion(protoimpl.MaxVersion - 20)
)

type Mutex_Enum int32

const (
	Mutex_none Mutex_Enum = 0
	Mutex_zero Mutex_Enum = 1
	Mutex_one  Mutex_Enum = 2
)

// Enum value maps for Mutex_Enum.
var (
	Mutex_Enum_name = map[int32]string{
		0: "none",
		1: "zero",
		2: "one",
	}
	Mutex_Enum_value = map[string]int32{
		"none": 0,
		"zero": 1,
		"one":  2,
	}
)

func (x Mutex_Enum) Enum() *Mutex_Enum {
	p := new(Mutex_Enum)
	*p = x
	return p
}

func (x Mutex_Enum) String() string {
	return protoimpl.X.EnumStringOf(x.Descriptor(), protoreflect.EnumNumber(x))
}

func (Mutex_Enum) Descriptor() protoreflect.EnumDescriptor {
	return file_msg_proto_enumTypes[0].Descriptor()
}

func (Mutex_Enum) Type() protoreflect.EnumType {
	return &file_msg_proto_enumTypes[0]
}

func (x Mutex_Enum) Number() protoreflect.EnumNumber {
	return protoreflect.EnumNumber(x)
}

// Deprecated: Use Mutex_Enum.Descriptor instead.
func (Mutex_Enum) EnumDescriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{0, 0}
}

type Mutex struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Mutex Mutex_Enum `protobuf:"varint,1,opt,name=mutex,proto3,enum=kase.Mutex_Enum" json:"mutex,omitempty"`
}

func (x *Mutex) Reset() {
	*x = Mutex{}
	if protoimpl.UnsafeEnabled {
		mi := &file_msg_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Mutex) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Mutex) ProtoMessage() {}

func (x *Mutex) ProtoReflect() protoreflect.Message {
	mi := &file_msg_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Mutex.ProtoReflect.Descriptor instead.
func (*Mutex) Descriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{0}
}

func (x *Mutex) GetMutex() Mutex_Enum {
	if x != nil {
		return x.Mutex
	}
	return Mutex_none
}

type BSI struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Int    int64   `protobuf:"varint,1,opt,name=int,proto3" json:"int,omitempty"`
	Double float64 `protobuf:"fixed64,2,opt,name=double,proto3" json:"double,omitempty"`
}

func (x *BSI) Reset() {
	*x = BSI{}
	if protoimpl.UnsafeEnabled {
		mi := &file_msg_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *BSI) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*BSI) ProtoMessage() {}

func (x *BSI) ProtoReflect() protoreflect.Message {
	mi := &file_msg_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use BSI.ProtoReflect.Descriptor instead.
func (*BSI) Descriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{1}
}

func (x *BSI) GetInt() int64 {
	if x != nil {
		return x.Int
	}
	return 0
}

func (x *BSI) GetDouble() float64 {
	if x != nil {
		return x.Double
	}
	return 0
}

type Bool struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Bool bool `protobuf:"varint,1,opt,name=bool,proto3" json:"bool,omitempty"`
}

func (x *Bool) Reset() {
	*x = Bool{}
	if protoimpl.UnsafeEnabled {
		mi := &file_msg_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Bool) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Bool) ProtoMessage() {}

func (x *Bool) ProtoReflect() protoreflect.Message {
	mi := &file_msg_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Bool.ProtoReflect.Descriptor instead.
func (*Bool) Descriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{2}
}

func (x *Bool) GetBool() bool {
	if x != nil {
		return x.Bool
	}
	return false
}

type String struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	String_ string `protobuf:"bytes,1,opt,name=string,proto3" json:"string,omitempty"`
}

func (x *String) Reset() {
	*x = String{}
	if protoimpl.UnsafeEnabled {
		mi := &file_msg_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *String) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*String) ProtoMessage() {}

func (x *String) ProtoReflect() protoreflect.Message {
	mi := &file_msg_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use String.ProtoReflect.Descriptor instead.
func (*String) Descriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{3}
}

func (x *String) GetString_() string {
	if x != nil {
		return x.String_
	}
	return ""
}

type Blob struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Blob []byte `protobuf:"bytes,1,opt,name=blob,proto3" json:"blob,omitempty"`
}

func (x *Blob) Reset() {
	*x = Blob{}
	if protoimpl.UnsafeEnabled {
		mi := &file_msg_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Blob) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Blob) ProtoMessage() {}

func (x *Blob) ProtoReflect() protoreflect.Message {
	mi := &file_msg_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Blob.ProtoReflect.Descriptor instead.
func (*Blob) Descriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{4}
}

func (x *Blob) GetBlob() []byte {
	if x != nil {
		return x.Blob
	}
	return nil
}

type Set struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	String_ []string `protobuf:"bytes,1,rep,name=string,proto3" json:"string,omitempty"`
	Uint64  [][]byte `protobuf:"bytes,2,rep,name=uint64,proto3" json:"uint64,omitempty"`
}

func (x *Set) Reset() {
	*x = Set{}
	if protoimpl.UnsafeEnabled {
		mi := &file_msg_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Set) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Set) ProtoMessage() {}

func (x *Set) ProtoReflect() protoreflect.Message {
	mi := &file_msg_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Set.ProtoReflect.Descriptor instead.
func (*Set) Descriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{5}
}

func (x *Set) GetString_() []string {
	if x != nil {
		return x.String_
	}
	return nil
}

func (x *Set) GetUint64() [][]byte {
	if x != nil {
		return x.Uint64
	}
	return nil
}

type TimestampMS struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamp int64 `protobuf:"varint,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
}

func (x *TimestampMS) Reset() {
	*x = TimestampMS{}
	if protoimpl.UnsafeEnabled {
		mi := &file_msg_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TimestampMS) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TimestampMS) ProtoMessage() {}

func (x *TimestampMS) ProtoReflect() protoreflect.Message {
	mi := &file_msg_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TimestampMS.ProtoReflect.Descriptor instead.
func (*TimestampMS) Descriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{6}
}

func (x *TimestampMS) GetTimestamp() int64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

type TimestampNS struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Timestamp uint64 `protobuf:"varint,1,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
}

func (x *TimestampNS) Reset() {
	*x = TimestampNS{}
	if protoimpl.UnsafeEnabled {
		mi := &file_msg_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TimestampNS) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TimestampNS) ProtoMessage() {}

func (x *TimestampNS) ProtoReflect() protoreflect.Message {
	mi := &file_msg_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TimestampNS.ProtoReflect.Descriptor instead.
func (*TimestampNS) Descriptor() ([]byte, []int) {
	return file_msg_proto_rawDescGZIP(), []int{7}
}

func (x *TimestampNS) GetTimestamp() uint64 {
	if x != nil {
		return x.Timestamp
	}
	return 0
}

var File_msg_proto protoreflect.FileDescriptor

var file_msg_proto_rawDesc = []byte{
	0x0a, 0x09, 0x6d, 0x73, 0x67, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x04, 0x6b, 0x61, 0x73,
	0x65, 0x22, 0x54, 0x0a, 0x05, 0x4d, 0x75, 0x74, 0x65, 0x78, 0x12, 0x26, 0x0a, 0x05, 0x6d, 0x75,
	0x74, 0x65, 0x78, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x10, 0x2e, 0x6b, 0x61, 0x73, 0x65,
	0x2e, 0x4d, 0x75, 0x74, 0x65, 0x78, 0x2e, 0x45, 0x6e, 0x75, 0x6d, 0x52, 0x05, 0x6d, 0x75, 0x74,
	0x65, 0x78, 0x22, 0x23, 0x0a, 0x04, 0x45, 0x6e, 0x75, 0x6d, 0x12, 0x08, 0x0a, 0x04, 0x6e, 0x6f,
	0x6e, 0x65, 0x10, 0x00, 0x12, 0x08, 0x0a, 0x04, 0x7a, 0x65, 0x72, 0x6f, 0x10, 0x01, 0x12, 0x07,
	0x0a, 0x03, 0x6f, 0x6e, 0x65, 0x10, 0x02, 0x22, 0x2f, 0x0a, 0x03, 0x42, 0x53, 0x49, 0x12, 0x10,
	0x0a, 0x03, 0x69, 0x6e, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x03, 0x69, 0x6e, 0x74,
	0x12, 0x16, 0x0a, 0x06, 0x64, 0x6f, 0x75, 0x62, 0x6c, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x01,
	0x52, 0x06, 0x64, 0x6f, 0x75, 0x62, 0x6c, 0x65, 0x22, 0x1a, 0x0a, 0x04, 0x42, 0x6f, 0x6f, 0x6c,
	0x12, 0x12, 0x0a, 0x04, 0x62, 0x6f, 0x6f, 0x6c, 0x18, 0x01, 0x20, 0x01, 0x28, 0x08, 0x52, 0x04,
	0x62, 0x6f, 0x6f, 0x6c, 0x22, 0x20, 0x0a, 0x06, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x12, 0x16,
	0x0a, 0x06, 0x73, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x06,
	0x73, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x22, 0x1a, 0x0a, 0x04, 0x42, 0x6c, 0x6f, 0x62, 0x12, 0x12,
	0x0a, 0x04, 0x62, 0x6c, 0x6f, 0x62, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x52, 0x04, 0x62, 0x6c,
	0x6f, 0x62, 0x22, 0x35, 0x0a, 0x03, 0x53, 0x65, 0x74, 0x12, 0x16, 0x0a, 0x06, 0x73, 0x74, 0x72,
	0x69, 0x6e, 0x67, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x06, 0x73, 0x74, 0x72, 0x69, 0x6e,
	0x67, 0x12, 0x16, 0x0a, 0x06, 0x75, 0x69, 0x6e, 0x74, 0x36, 0x34, 0x18, 0x02, 0x20, 0x03, 0x28,
	0x0c, 0x52, 0x06, 0x75, 0x69, 0x6e, 0x74, 0x36, 0x34, 0x22, 0x2b, 0x0a, 0x0b, 0x54, 0x69, 0x6d,
	0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x4d, 0x53, 0x12, 0x1c, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65,
	0x73, 0x74, 0x61, 0x6d, 0x70, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x09, 0x74, 0x69, 0x6d,
	0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x22, 0x2b, 0x0a, 0x0b, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74,
	0x61, 0x6d, 0x70, 0x4e, 0x53, 0x12, 0x1c, 0x0a, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61,
	0x6d, 0x70, 0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x52, 0x09, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74,
	0x61, 0x6d, 0x70, 0x42, 0x21, 0x5a, 0x1f, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e, 0x63, 0x6f,
	0x6d, 0x2f, 0x67, 0x65, 0x72, 0x6e, 0x65, 0x73, 0x74, 0x2f, 0x72, 0x62, 0x66, 0x2f, 0x64, 0x73,
	0x6c, 0x2f, 0x6b, 0x61, 0x73, 0x65, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_msg_proto_rawDescOnce sync.Once
	file_msg_proto_rawDescData = file_msg_proto_rawDesc
)

func file_msg_proto_rawDescGZIP() []byte {
	file_msg_proto_rawDescOnce.Do(func() {
		file_msg_proto_rawDescData = protoimpl.X.CompressGZIP(file_msg_proto_rawDescData)
	})
	return file_msg_proto_rawDescData
}

var file_msg_proto_enumTypes = make([]protoimpl.EnumInfo, 1)
var file_msg_proto_msgTypes = make([]protoimpl.MessageInfo, 8)
var file_msg_proto_goTypes = []interface{}{
	(Mutex_Enum)(0),     // 0: kase.Mutex.Enum
	(*Mutex)(nil),       // 1: kase.Mutex
	(*BSI)(nil),         // 2: kase.BSI
	(*Bool)(nil),        // 3: kase.Bool
	(*String)(nil),      // 4: kase.String
	(*Blob)(nil),        // 5: kase.Blob
	(*Set)(nil),         // 6: kase.Set
	(*TimestampMS)(nil), // 7: kase.TimestampMS
	(*TimestampNS)(nil), // 8: kase.TimestampNS
}
var file_msg_proto_depIdxs = []int32{
	0, // 0: kase.Mutex.mutex:type_name -> kase.Mutex.Enum
	1, // [1:1] is the sub-list for method output_type
	1, // [1:1] is the sub-list for method input_type
	1, // [1:1] is the sub-list for extension type_name
	1, // [1:1] is the sub-list for extension extendee
	0, // [0:1] is the sub-list for field type_name
}

func init() { file_msg_proto_init() }
func file_msg_proto_init() {
	if File_msg_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_msg_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Mutex); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_msg_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*BSI); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_msg_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Bool); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_msg_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*String); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_msg_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Blob); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_msg_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Set); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_msg_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TimestampMS); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
		file_msg_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TimestampNS); i {
			case 0:
				return &v.state
			case 1:
				return &v.sizeCache
			case 2:
				return &v.unknownFields
			default:
				return nil
			}
		}
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_msg_proto_rawDesc,
			NumEnums:      1,
			NumMessages:   8,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_msg_proto_goTypes,
		DependencyIndexes: file_msg_proto_depIdxs,
		EnumInfos:         file_msg_proto_enumTypes,
		MessageInfos:      file_msg_proto_msgTypes,
	}.Build()
	File_msg_proto = out.File
	file_msg_proto_rawDesc = nil
	file_msg_proto_goTypes = nil
	file_msg_proto_depIdxs = nil
}
