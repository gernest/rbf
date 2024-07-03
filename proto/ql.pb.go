// Code generated by protoc-gen-go. DO NOT EDIT.
// versions:
// 	protoc-gen-go v1.30.0
// 	protoc        v3.21.12
// source: ql.proto

package proto

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

type QueryPQLRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Index string `protobuf:"bytes,1,opt,name=index,proto3" json:"index,omitempty"`
	Pql   string `protobuf:"bytes,2,opt,name=pql,proto3" json:"pql,omitempty"`
}

func (x *QueryPQLRequest) Reset() {
	*x = QueryPQLRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ql_proto_msgTypes[0]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QueryPQLRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QueryPQLRequest) ProtoMessage() {}

func (x *QueryPQLRequest) ProtoReflect() protoreflect.Message {
	mi := &file_ql_proto_msgTypes[0]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QueryPQLRequest.ProtoReflect.Descriptor instead.
func (*QueryPQLRequest) Descriptor() ([]byte, []int) {
	return file_ql_proto_rawDescGZIP(), []int{0}
}

func (x *QueryPQLRequest) GetIndex() string {
	if x != nil {
		return x.Index
	}
	return ""
}

func (x *QueryPQLRequest) GetPql() string {
	if x != nil {
		return x.Pql
	}
	return ""
}

type QuerySQLRequest struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Sql string `protobuf:"bytes,1,opt,name=sql,proto3" json:"sql,omitempty"`
}

func (x *QuerySQLRequest) Reset() {
	*x = QuerySQLRequest{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ql_proto_msgTypes[1]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *QuerySQLRequest) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*QuerySQLRequest) ProtoMessage() {}

func (x *QuerySQLRequest) ProtoReflect() protoreflect.Message {
	mi := &file_ql_proto_msgTypes[1]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use QuerySQLRequest.ProtoReflect.Descriptor instead.
func (*QuerySQLRequest) Descriptor() ([]byte, []int) {
	return file_ql_proto_rawDescGZIP(), []int{1}
}

func (x *QuerySQLRequest) GetSql() string {
	if x != nil {
		return x.Sql
	}
	return ""
}

type StatusError struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Code    uint32 `protobuf:"varint,1,opt,name=Code,proto3" json:"Code,omitempty"`
	Message string `protobuf:"bytes,2,opt,name=Message,proto3" json:"Message,omitempty"`
}

func (x *StatusError) Reset() {
	*x = StatusError{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ql_proto_msgTypes[2]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StatusError) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StatusError) ProtoMessage() {}

func (x *StatusError) ProtoReflect() protoreflect.Message {
	mi := &file_ql_proto_msgTypes[2]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StatusError.ProtoReflect.Descriptor instead.
func (*StatusError) Descriptor() ([]byte, []int) {
	return file_ql_proto_rawDescGZIP(), []int{2}
}

func (x *StatusError) GetCode() uint32 {
	if x != nil {
		return x.Code
	}
	return 0
}

func (x *StatusError) GetMessage() string {
	if x != nil {
		return x.Message
	}
	return ""
}

type RowResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Headers     []*ColumnInfo     `protobuf:"bytes,1,rep,name=headers,proto3" json:"headers,omitempty"`
	Columns     []*ColumnResponse `protobuf:"bytes,2,rep,name=columns,proto3" json:"columns,omitempty"`
	StatusError *StatusError      `protobuf:"bytes,3,opt,name=StatusError,proto3" json:"StatusError,omitempty"`
	Duration    int64             `protobuf:"varint,4,opt,name=duration,proto3" json:"duration,omitempty"`
}

func (x *RowResponse) Reset() {
	*x = RowResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ql_proto_msgTypes[3]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *RowResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*RowResponse) ProtoMessage() {}

func (x *RowResponse) ProtoReflect() protoreflect.Message {
	mi := &file_ql_proto_msgTypes[3]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use RowResponse.ProtoReflect.Descriptor instead.
func (*RowResponse) Descriptor() ([]byte, []int) {
	return file_ql_proto_rawDescGZIP(), []int{3}
}

func (x *RowResponse) GetHeaders() []*ColumnInfo {
	if x != nil {
		return x.Headers
	}
	return nil
}

func (x *RowResponse) GetColumns() []*ColumnResponse {
	if x != nil {
		return x.Columns
	}
	return nil
}

func (x *RowResponse) GetStatusError() *StatusError {
	if x != nil {
		return x.StatusError
	}
	return nil
}

func (x *RowResponse) GetDuration() int64 {
	if x != nil {
		return x.Duration
	}
	return 0
}

type Row struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Columns []*ColumnResponse `protobuf:"bytes,1,rep,name=columns,proto3" json:"columns,omitempty"`
}

func (x *Row) Reset() {
	*x = Row{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ql_proto_msgTypes[4]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Row) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Row) ProtoMessage() {}

func (x *Row) ProtoReflect() protoreflect.Message {
	mi := &file_ql_proto_msgTypes[4]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Row.ProtoReflect.Descriptor instead.
func (*Row) Descriptor() ([]byte, []int) {
	return file_ql_proto_rawDescGZIP(), []int{4}
}

func (x *Row) GetColumns() []*ColumnResponse {
	if x != nil {
		return x.Columns
	}
	return nil
}

type TableResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Headers     []*ColumnInfo `protobuf:"bytes,1,rep,name=headers,proto3" json:"headers,omitempty"`
	Rows        []*Row        `protobuf:"bytes,2,rep,name=rows,proto3" json:"rows,omitempty"`
	StatusError *StatusError  `protobuf:"bytes,3,opt,name=StatusError,proto3" json:"StatusError,omitempty"`
	Duration    int64         `protobuf:"varint,4,opt,name=duration,proto3" json:"duration,omitempty"`
}

func (x *TableResponse) Reset() {
	*x = TableResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ql_proto_msgTypes[5]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *TableResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*TableResponse) ProtoMessage() {}

func (x *TableResponse) ProtoReflect() protoreflect.Message {
	mi := &file_ql_proto_msgTypes[5]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use TableResponse.ProtoReflect.Descriptor instead.
func (*TableResponse) Descriptor() ([]byte, []int) {
	return file_ql_proto_rawDescGZIP(), []int{5}
}

func (x *TableResponse) GetHeaders() []*ColumnInfo {
	if x != nil {
		return x.Headers
	}
	return nil
}

func (x *TableResponse) GetRows() []*Row {
	if x != nil {
		return x.Rows
	}
	return nil
}

func (x *TableResponse) GetStatusError() *StatusError {
	if x != nil {
		return x.StatusError
	}
	return nil
}

func (x *TableResponse) GetDuration() int64 {
	if x != nil {
		return x.Duration
	}
	return 0
}

type ColumnInfo struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Name     string `protobuf:"bytes,1,opt,name=name,proto3" json:"name,omitempty"`
	Datatype string `protobuf:"bytes,2,opt,name=datatype,proto3" json:"datatype,omitempty"`
}

func (x *ColumnInfo) Reset() {
	*x = ColumnInfo{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ql_proto_msgTypes[6]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ColumnInfo) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ColumnInfo) ProtoMessage() {}

func (x *ColumnInfo) ProtoReflect() protoreflect.Message {
	mi := &file_ql_proto_msgTypes[6]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ColumnInfo.ProtoReflect.Descriptor instead.
func (*ColumnInfo) Descriptor() ([]byte, []int) {
	return file_ql_proto_rawDescGZIP(), []int{6}
}

func (x *ColumnInfo) GetName() string {
	if x != nil {
		return x.Name
	}
	return ""
}

func (x *ColumnInfo) GetDatatype() string {
	if x != nil {
		return x.Datatype
	}
	return ""
}

type ColumnResponse struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to ColumnVal:
	//
	//	*ColumnResponse_StringVal
	//	*ColumnResponse_Uint64Val
	//	*ColumnResponse_Int64Val
	//	*ColumnResponse_BoolVal
	//	*ColumnResponse_BlobVal
	//	*ColumnResponse_Uint64ArrayVal
	//	*ColumnResponse_StringArrayVal
	//	*ColumnResponse_Float64Val
	//	*ColumnResponse_DecimalVal
	//	*ColumnResponse_TimestampVal
	ColumnVal isColumnResponse_ColumnVal `protobuf_oneof:"columnVal"`
}

func (x *ColumnResponse) Reset() {
	*x = ColumnResponse{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ql_proto_msgTypes[7]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *ColumnResponse) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*ColumnResponse) ProtoMessage() {}

func (x *ColumnResponse) ProtoReflect() protoreflect.Message {
	mi := &file_ql_proto_msgTypes[7]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use ColumnResponse.ProtoReflect.Descriptor instead.
func (*ColumnResponse) Descriptor() ([]byte, []int) {
	return file_ql_proto_rawDescGZIP(), []int{7}
}

func (m *ColumnResponse) GetColumnVal() isColumnResponse_ColumnVal {
	if m != nil {
		return m.ColumnVal
	}
	return nil
}

func (x *ColumnResponse) GetStringVal() string {
	if x, ok := x.GetColumnVal().(*ColumnResponse_StringVal); ok {
		return x.StringVal
	}
	return ""
}

func (x *ColumnResponse) GetUint64Val() uint64 {
	if x, ok := x.GetColumnVal().(*ColumnResponse_Uint64Val); ok {
		return x.Uint64Val
	}
	return 0
}

func (x *ColumnResponse) GetInt64Val() int64 {
	if x, ok := x.GetColumnVal().(*ColumnResponse_Int64Val); ok {
		return x.Int64Val
	}
	return 0
}

func (x *ColumnResponse) GetBoolVal() bool {
	if x, ok := x.GetColumnVal().(*ColumnResponse_BoolVal); ok {
		return x.BoolVal
	}
	return false
}

func (x *ColumnResponse) GetBlobVal() []byte {
	if x, ok := x.GetColumnVal().(*ColumnResponse_BlobVal); ok {
		return x.BlobVal
	}
	return nil
}

func (x *ColumnResponse) GetUint64ArrayVal() *Uint64Array {
	if x, ok := x.GetColumnVal().(*ColumnResponse_Uint64ArrayVal); ok {
		return x.Uint64ArrayVal
	}
	return nil
}

func (x *ColumnResponse) GetStringArrayVal() *StringArray {
	if x, ok := x.GetColumnVal().(*ColumnResponse_StringArrayVal); ok {
		return x.StringArrayVal
	}
	return nil
}

func (x *ColumnResponse) GetFloat64Val() float64 {
	if x, ok := x.GetColumnVal().(*ColumnResponse_Float64Val); ok {
		return x.Float64Val
	}
	return 0
}

func (x *ColumnResponse) GetDecimalVal() *Decimal {
	if x, ok := x.GetColumnVal().(*ColumnResponse_DecimalVal); ok {
		return x.DecimalVal
	}
	return nil
}

func (x *ColumnResponse) GetTimestampVal() string {
	if x, ok := x.GetColumnVal().(*ColumnResponse_TimestampVal); ok {
		return x.TimestampVal
	}
	return ""
}

type isColumnResponse_ColumnVal interface {
	isColumnResponse_ColumnVal()
}

type ColumnResponse_StringVal struct {
	StringVal string `protobuf:"bytes,1,opt,name=stringVal,proto3,oneof"`
}

type ColumnResponse_Uint64Val struct {
	Uint64Val uint64 `protobuf:"varint,2,opt,name=uint64Val,proto3,oneof"`
}

type ColumnResponse_Int64Val struct {
	Int64Val int64 `protobuf:"varint,3,opt,name=int64Val,proto3,oneof"`
}

type ColumnResponse_BoolVal struct {
	BoolVal bool `protobuf:"varint,4,opt,name=boolVal,proto3,oneof"`
}

type ColumnResponse_BlobVal struct {
	BlobVal []byte `protobuf:"bytes,5,opt,name=blobVal,proto3,oneof"`
}

type ColumnResponse_Uint64ArrayVal struct {
	Uint64ArrayVal *Uint64Array `protobuf:"bytes,6,opt,name=uint64ArrayVal,proto3,oneof"`
}

type ColumnResponse_StringArrayVal struct {
	StringArrayVal *StringArray `protobuf:"bytes,7,opt,name=stringArrayVal,proto3,oneof"`
}

type ColumnResponse_Float64Val struct {
	Float64Val float64 `protobuf:"fixed64,8,opt,name=float64Val,proto3,oneof"`
}

type ColumnResponse_DecimalVal struct {
	DecimalVal *Decimal `protobuf:"bytes,9,opt,name=decimalVal,proto3,oneof"`
}

type ColumnResponse_TimestampVal struct {
	TimestampVal string `protobuf:"bytes,10,opt,name=timestampVal,proto3,oneof"`
}

func (*ColumnResponse_StringVal) isColumnResponse_ColumnVal() {}

func (*ColumnResponse_Uint64Val) isColumnResponse_ColumnVal() {}

func (*ColumnResponse_Int64Val) isColumnResponse_ColumnVal() {}

func (*ColumnResponse_BoolVal) isColumnResponse_ColumnVal() {}

func (*ColumnResponse_BlobVal) isColumnResponse_ColumnVal() {}

func (*ColumnResponse_Uint64ArrayVal) isColumnResponse_ColumnVal() {}

func (*ColumnResponse_StringArrayVal) isColumnResponse_ColumnVal() {}

func (*ColumnResponse_Float64Val) isColumnResponse_ColumnVal() {}

func (*ColumnResponse_DecimalVal) isColumnResponse_ColumnVal() {}

func (*ColumnResponse_TimestampVal) isColumnResponse_ColumnVal() {}

type Decimal struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Value int64 `protobuf:"varint,1,opt,name=value,proto3" json:"value,omitempty"`
	Scale int64 `protobuf:"varint,2,opt,name=scale,proto3" json:"scale,omitempty"`
}

func (x *Decimal) Reset() {
	*x = Decimal{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ql_proto_msgTypes[8]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Decimal) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Decimal) ProtoMessage() {}

func (x *Decimal) ProtoReflect() protoreflect.Message {
	mi := &file_ql_proto_msgTypes[8]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Decimal.ProtoReflect.Descriptor instead.
func (*Decimal) Descriptor() ([]byte, []int) {
	return file_ql_proto_rawDescGZIP(), []int{8}
}

func (x *Decimal) GetValue() int64 {
	if x != nil {
		return x.Value
	}
	return 0
}

func (x *Decimal) GetScale() int64 {
	if x != nil {
		return x.Scale
	}
	return 0
}

type Uint64Array struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Vals []uint64 `protobuf:"varint,1,rep,packed,name=vals,proto3" json:"vals,omitempty"`
}

func (x *Uint64Array) Reset() {
	*x = Uint64Array{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ql_proto_msgTypes[9]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *Uint64Array) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*Uint64Array) ProtoMessage() {}

func (x *Uint64Array) ProtoReflect() protoreflect.Message {
	mi := &file_ql_proto_msgTypes[9]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use Uint64Array.ProtoReflect.Descriptor instead.
func (*Uint64Array) Descriptor() ([]byte, []int) {
	return file_ql_proto_rawDescGZIP(), []int{9}
}

func (x *Uint64Array) GetVals() []uint64 {
	if x != nil {
		return x.Vals
	}
	return nil
}

type StringArray struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	Vals []string `protobuf:"bytes,1,rep,name=vals,proto3" json:"vals,omitempty"`
}

func (x *StringArray) Reset() {
	*x = StringArray{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ql_proto_msgTypes[10]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *StringArray) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*StringArray) ProtoMessage() {}

func (x *StringArray) ProtoReflect() protoreflect.Message {
	mi := &file_ql_proto_msgTypes[10]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use StringArray.ProtoReflect.Descriptor instead.
func (*StringArray) Descriptor() ([]byte, []int) {
	return file_ql_proto_rawDescGZIP(), []int{10}
}

func (x *StringArray) GetVals() []string {
	if x != nil {
		return x.Vals
	}
	return nil
}

type IdsOrKeys struct {
	state         protoimpl.MessageState
	sizeCache     protoimpl.SizeCache
	unknownFields protoimpl.UnknownFields

	// Types that are assignable to Type:
	//
	//	*IdsOrKeys_Ids
	//	*IdsOrKeys_Keys
	Type isIdsOrKeys_Type `protobuf_oneof:"type"`
}

func (x *IdsOrKeys) Reset() {
	*x = IdsOrKeys{}
	if protoimpl.UnsafeEnabled {
		mi := &file_ql_proto_msgTypes[11]
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		ms.StoreMessageInfo(mi)
	}
}

func (x *IdsOrKeys) String() string {
	return protoimpl.X.MessageStringOf(x)
}

func (*IdsOrKeys) ProtoMessage() {}

func (x *IdsOrKeys) ProtoReflect() protoreflect.Message {
	mi := &file_ql_proto_msgTypes[11]
	if protoimpl.UnsafeEnabled && x != nil {
		ms := protoimpl.X.MessageStateOf(protoimpl.Pointer(x))
		if ms.LoadMessageInfo() == nil {
			ms.StoreMessageInfo(mi)
		}
		return ms
	}
	return mi.MessageOf(x)
}

// Deprecated: Use IdsOrKeys.ProtoReflect.Descriptor instead.
func (*IdsOrKeys) Descriptor() ([]byte, []int) {
	return file_ql_proto_rawDescGZIP(), []int{11}
}

func (m *IdsOrKeys) GetType() isIdsOrKeys_Type {
	if m != nil {
		return m.Type
	}
	return nil
}

func (x *IdsOrKeys) GetIds() *Uint64Array {
	if x, ok := x.GetType().(*IdsOrKeys_Ids); ok {
		return x.Ids
	}
	return nil
}

func (x *IdsOrKeys) GetKeys() *StringArray {
	if x, ok := x.GetType().(*IdsOrKeys_Keys); ok {
		return x.Keys
	}
	return nil
}

type isIdsOrKeys_Type interface {
	isIdsOrKeys_Type()
}

type IdsOrKeys_Ids struct {
	Ids *Uint64Array `protobuf:"bytes,1,opt,name=ids,proto3,oneof"`
}

type IdsOrKeys_Keys struct {
	Keys *StringArray `protobuf:"bytes,2,opt,name=keys,proto3,oneof"`
}

func (*IdsOrKeys_Ids) isIdsOrKeys_Type() {}

func (*IdsOrKeys_Keys) isIdsOrKeys_Type() {}

var File_ql_proto protoreflect.FileDescriptor

var file_ql_proto_rawDesc = []byte{
	0x0a, 0x08, 0x71, 0x6c, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x05, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x22, 0x39, 0x0a, 0x0f, 0x51, 0x75, 0x65, 0x72, 0x79, 0x50, 0x51, 0x4c, 0x52, 0x65, 0x71,
	0x75, 0x65, 0x73, 0x74, 0x12, 0x14, 0x0a, 0x05, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x05, 0x69, 0x6e, 0x64, 0x65, 0x78, 0x12, 0x10, 0x0a, 0x03, 0x70, 0x71,
	0x6c, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x70, 0x71, 0x6c, 0x22, 0x23, 0x0a, 0x0f,
	0x51, 0x75, 0x65, 0x72, 0x79, 0x53, 0x51, 0x4c, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
	0x10, 0x0a, 0x03, 0x73, 0x71, 0x6c, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x52, 0x03, 0x73, 0x71,
	0x6c, 0x22, 0x3b, 0x0a, 0x0b, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x45, 0x72, 0x72, 0x6f, 0x72,
	0x12, 0x12, 0x0a, 0x04, 0x43, 0x6f, 0x64, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0d, 0x52, 0x04,
	0x43, 0x6f, 0x64, 0x65, 0x12, 0x18, 0x0a, 0x07, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18,
	0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x07, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x22, 0xbd,
	0x01, 0x0a, 0x0b, 0x52, 0x6f, 0x77, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x2b,
	0x0a, 0x07, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32,
	0x11, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x49, 0x6e,
	0x66, 0x6f, 0x52, 0x07, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x73, 0x12, 0x2f, 0x0a, 0x07, 0x63,
	0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x70,
	0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f,
	0x6e, 0x73, 0x65, 0x52, 0x07, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73, 0x12, 0x34, 0x0a, 0x0b,
	0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x12, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73,
	0x45, 0x72, 0x72, 0x6f, 0x72, 0x52, 0x0b, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x45, 0x72, 0x72,
	0x6f, 0x72, 0x12, 0x1a, 0x0a, 0x08, 0x64, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x04,
	0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x64, 0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0x36,
	0x0a, 0x03, 0x52, 0x6f, 0x77, 0x12, 0x2f, 0x0a, 0x07, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73,
	0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x43,
	0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x52, 0x07, 0x63,
	0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x73, 0x22, 0xae, 0x01, 0x0a, 0x0d, 0x54, 0x61, 0x62, 0x6c, 0x65,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x2b, 0x0a, 0x07, 0x68, 0x65, 0x61, 0x64,
	0x65, 0x72, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x70, 0x72, 0x6f, 0x74,
	0x6f, 0x2e, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x49, 0x6e, 0x66, 0x6f, 0x52, 0x07, 0x68, 0x65,
	0x61, 0x64, 0x65, 0x72, 0x73, 0x12, 0x1e, 0x0a, 0x04, 0x72, 0x6f, 0x77, 0x73, 0x18, 0x02, 0x20,
	0x03, 0x28, 0x0b, 0x32, 0x0a, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x52, 0x6f, 0x77, 0x52,
	0x04, 0x72, 0x6f, 0x77, 0x73, 0x12, 0x34, 0x0a, 0x0b, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x45,
	0x72, 0x72, 0x6f, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x70, 0x72, 0x6f,
	0x74, 0x6f, 0x2e, 0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x52, 0x0b,
	0x53, 0x74, 0x61, 0x74, 0x75, 0x73, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x12, 0x1a, 0x0a, 0x08, 0x64,
	0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x18, 0x04, 0x20, 0x01, 0x28, 0x03, 0x52, 0x08, 0x64,
	0x75, 0x72, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x22, 0x3c, 0x0a, 0x0a, 0x43, 0x6f, 0x6c, 0x75, 0x6d,
	0x6e, 0x49, 0x6e, 0x66, 0x6f, 0x12, 0x12, 0x0a, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x18, 0x01, 0x20,
	0x01, 0x28, 0x09, 0x52, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x12, 0x1a, 0x0a, 0x08, 0x64, 0x61, 0x74,
	0x61, 0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x09, 0x52, 0x08, 0x64, 0x61, 0x74,
	0x61, 0x74, 0x79, 0x70, 0x65, 0x22, 0xa9, 0x03, 0x0a, 0x0e, 0x43, 0x6f, 0x6c, 0x75, 0x6d, 0x6e,
	0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x1e, 0x0a, 0x09, 0x73, 0x74, 0x72, 0x69,
	0x6e, 0x67, 0x56, 0x61, 0x6c, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x09, 0x73,
	0x74, 0x72, 0x69, 0x6e, 0x67, 0x56, 0x61, 0x6c, 0x12, 0x1e, 0x0a, 0x09, 0x75, 0x69, 0x6e, 0x74,
	0x36, 0x34, 0x56, 0x61, 0x6c, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x48, 0x00, 0x52, 0x09, 0x75,
	0x69, 0x6e, 0x74, 0x36, 0x34, 0x56, 0x61, 0x6c, 0x12, 0x1c, 0x0a, 0x08, 0x69, 0x6e, 0x74, 0x36,
	0x34, 0x56, 0x61, 0x6c, 0x18, 0x03, 0x20, 0x01, 0x28, 0x03, 0x48, 0x00, 0x52, 0x08, 0x69, 0x6e,
	0x74, 0x36, 0x34, 0x56, 0x61, 0x6c, 0x12, 0x1a, 0x0a, 0x07, 0x62, 0x6f, 0x6f, 0x6c, 0x56, 0x61,
	0x6c, 0x18, 0x04, 0x20, 0x01, 0x28, 0x08, 0x48, 0x00, 0x52, 0x07, 0x62, 0x6f, 0x6f, 0x6c, 0x56,
	0x61, 0x6c, 0x12, 0x1a, 0x0a, 0x07, 0x62, 0x6c, 0x6f, 0x62, 0x56, 0x61, 0x6c, 0x18, 0x05, 0x20,
	0x01, 0x28, 0x0c, 0x48, 0x00, 0x52, 0x07, 0x62, 0x6c, 0x6f, 0x62, 0x56, 0x61, 0x6c, 0x12, 0x3c,
	0x0a, 0x0e, 0x75, 0x69, 0x6e, 0x74, 0x36, 0x34, 0x41, 0x72, 0x72, 0x61, 0x79, 0x56, 0x61, 0x6c,
	0x18, 0x06, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x55,
	0x69, 0x6e, 0x74, 0x36, 0x34, 0x41, 0x72, 0x72, 0x61, 0x79, 0x48, 0x00, 0x52, 0x0e, 0x75, 0x69,
	0x6e, 0x74, 0x36, 0x34, 0x41, 0x72, 0x72, 0x61, 0x79, 0x56, 0x61, 0x6c, 0x12, 0x3c, 0x0a, 0x0e,
	0x73, 0x74, 0x72, 0x69, 0x6e, 0x67, 0x41, 0x72, 0x72, 0x61, 0x79, 0x56, 0x61, 0x6c, 0x18, 0x07,
	0x20, 0x01, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x53, 0x74, 0x72,
	0x69, 0x6e, 0x67, 0x41, 0x72, 0x72, 0x61, 0x79, 0x48, 0x00, 0x52, 0x0e, 0x73, 0x74, 0x72, 0x69,
	0x6e, 0x67, 0x41, 0x72, 0x72, 0x61, 0x79, 0x56, 0x61, 0x6c, 0x12, 0x20, 0x0a, 0x0a, 0x66, 0x6c,
	0x6f, 0x61, 0x74, 0x36, 0x34, 0x56, 0x61, 0x6c, 0x18, 0x08, 0x20, 0x01, 0x28, 0x01, 0x48, 0x00,
	0x52, 0x0a, 0x66, 0x6c, 0x6f, 0x61, 0x74, 0x36, 0x34, 0x56, 0x61, 0x6c, 0x12, 0x30, 0x0a, 0x0a,
	0x64, 0x65, 0x63, 0x69, 0x6d, 0x61, 0x6c, 0x56, 0x61, 0x6c, 0x18, 0x09, 0x20, 0x01, 0x28, 0x0b,
	0x32, 0x0e, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x44, 0x65, 0x63, 0x69, 0x6d, 0x61, 0x6c,
	0x48, 0x00, 0x52, 0x0a, 0x64, 0x65, 0x63, 0x69, 0x6d, 0x61, 0x6c, 0x56, 0x61, 0x6c, 0x12, 0x24,
	0x0a, 0x0c, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x56, 0x61, 0x6c, 0x18, 0x0a,
	0x20, 0x01, 0x28, 0x09, 0x48, 0x00, 0x52, 0x0c, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d,
	0x70, 0x56, 0x61, 0x6c, 0x42, 0x0b, 0x0a, 0x09, 0x63, 0x6f, 0x6c, 0x75, 0x6d, 0x6e, 0x56, 0x61,
	0x6c, 0x22, 0x35, 0x0a, 0x07, 0x44, 0x65, 0x63, 0x69, 0x6d, 0x61, 0x6c, 0x12, 0x14, 0x0a, 0x05,
	0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x03, 0x52, 0x05, 0x76, 0x61, 0x6c,
	0x75, 0x65, 0x12, 0x14, 0x0a, 0x05, 0x73, 0x63, 0x61, 0x6c, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x03, 0x52, 0x05, 0x73, 0x63, 0x61, 0x6c, 0x65, 0x22, 0x21, 0x0a, 0x0b, 0x55, 0x69, 0x6e, 0x74,
	0x36, 0x34, 0x41, 0x72, 0x72, 0x61, 0x79, 0x12, 0x12, 0x0a, 0x04, 0x76, 0x61, 0x6c, 0x73, 0x18,
	0x01, 0x20, 0x03, 0x28, 0x04, 0x52, 0x04, 0x76, 0x61, 0x6c, 0x73, 0x22, 0x21, 0x0a, 0x0b, 0x53,
	0x74, 0x72, 0x69, 0x6e, 0x67, 0x41, 0x72, 0x72, 0x61, 0x79, 0x12, 0x12, 0x0a, 0x04, 0x76, 0x61,
	0x6c, 0x73, 0x18, 0x01, 0x20, 0x03, 0x28, 0x09, 0x52, 0x04, 0x76, 0x61, 0x6c, 0x73, 0x22, 0x65,
	0x0a, 0x09, 0x49, 0x64, 0x73, 0x4f, 0x72, 0x4b, 0x65, 0x79, 0x73, 0x12, 0x26, 0x0a, 0x03, 0x69,
	0x64, 0x73, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x12, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
	0x2e, 0x55, 0x69, 0x6e, 0x74, 0x36, 0x34, 0x41, 0x72, 0x72, 0x61, 0x79, 0x48, 0x00, 0x52, 0x03,
	0x69, 0x64, 0x73, 0x12, 0x28, 0x0a, 0x04, 0x6b, 0x65, 0x79, 0x73, 0x18, 0x02, 0x20, 0x01, 0x28,
	0x0b, 0x32, 0x12, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x2e, 0x53, 0x74, 0x72, 0x69, 0x6e, 0x67,
	0x41, 0x72, 0x72, 0x61, 0x79, 0x48, 0x00, 0x52, 0x04, 0x6b, 0x65, 0x79, 0x73, 0x42, 0x06, 0x0a,
	0x04, 0x74, 0x79, 0x70, 0x65, 0x42, 0x1e, 0x5a, 0x1c, 0x67, 0x69, 0x74, 0x68, 0x75, 0x62, 0x2e,
	0x63, 0x6f, 0x6d, 0x2f, 0x67, 0x65, 0x72, 0x6e, 0x65, 0x73, 0x74, 0x2f, 0x72, 0x62, 0x66, 0x2f,
	0x70, 0x72, 0x6f, 0x74, 0x6f, 0x62, 0x06, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x33,
}

var (
	file_ql_proto_rawDescOnce sync.Once
	file_ql_proto_rawDescData = file_ql_proto_rawDesc
)

func file_ql_proto_rawDescGZIP() []byte {
	file_ql_proto_rawDescOnce.Do(func() {
		file_ql_proto_rawDescData = protoimpl.X.CompressGZIP(file_ql_proto_rawDescData)
	})
	return file_ql_proto_rawDescData
}

var file_ql_proto_msgTypes = make([]protoimpl.MessageInfo, 12)
var file_ql_proto_goTypes = []interface{}{
	(*QueryPQLRequest)(nil), // 0: proto.QueryPQLRequest
	(*QuerySQLRequest)(nil), // 1: proto.QuerySQLRequest
	(*StatusError)(nil),     // 2: proto.StatusError
	(*RowResponse)(nil),     // 3: proto.RowResponse
	(*Row)(nil),             // 4: proto.Row
	(*TableResponse)(nil),   // 5: proto.TableResponse
	(*ColumnInfo)(nil),      // 6: proto.ColumnInfo
	(*ColumnResponse)(nil),  // 7: proto.ColumnResponse
	(*Decimal)(nil),         // 8: proto.Decimal
	(*Uint64Array)(nil),     // 9: proto.Uint64Array
	(*StringArray)(nil),     // 10: proto.StringArray
	(*IdsOrKeys)(nil),       // 11: proto.IdsOrKeys
}
var file_ql_proto_depIdxs = []int32{
	6,  // 0: proto.RowResponse.headers:type_name -> proto.ColumnInfo
	7,  // 1: proto.RowResponse.columns:type_name -> proto.ColumnResponse
	2,  // 2: proto.RowResponse.StatusError:type_name -> proto.StatusError
	7,  // 3: proto.Row.columns:type_name -> proto.ColumnResponse
	6,  // 4: proto.TableResponse.headers:type_name -> proto.ColumnInfo
	4,  // 5: proto.TableResponse.rows:type_name -> proto.Row
	2,  // 6: proto.TableResponse.StatusError:type_name -> proto.StatusError
	9,  // 7: proto.ColumnResponse.uint64ArrayVal:type_name -> proto.Uint64Array
	10, // 8: proto.ColumnResponse.stringArrayVal:type_name -> proto.StringArray
	8,  // 9: proto.ColumnResponse.decimalVal:type_name -> proto.Decimal
	9,  // 10: proto.IdsOrKeys.ids:type_name -> proto.Uint64Array
	10, // 11: proto.IdsOrKeys.keys:type_name -> proto.StringArray
	12, // [12:12] is the sub-list for method output_type
	12, // [12:12] is the sub-list for method input_type
	12, // [12:12] is the sub-list for extension type_name
	12, // [12:12] is the sub-list for extension extendee
	0,  // [0:12] is the sub-list for field type_name
}

func init() { file_ql_proto_init() }
func file_ql_proto_init() {
	if File_ql_proto != nil {
		return
	}
	if !protoimpl.UnsafeEnabled {
		file_ql_proto_msgTypes[0].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*QueryPQLRequest); i {
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
		file_ql_proto_msgTypes[1].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*QuerySQLRequest); i {
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
		file_ql_proto_msgTypes[2].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StatusError); i {
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
		file_ql_proto_msgTypes[3].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*RowResponse); i {
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
		file_ql_proto_msgTypes[4].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Row); i {
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
		file_ql_proto_msgTypes[5].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*TableResponse); i {
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
		file_ql_proto_msgTypes[6].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ColumnInfo); i {
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
		file_ql_proto_msgTypes[7].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*ColumnResponse); i {
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
		file_ql_proto_msgTypes[8].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Decimal); i {
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
		file_ql_proto_msgTypes[9].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*Uint64Array); i {
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
		file_ql_proto_msgTypes[10].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*StringArray); i {
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
		file_ql_proto_msgTypes[11].Exporter = func(v interface{}, i int) interface{} {
			switch v := v.(*IdsOrKeys); i {
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
	file_ql_proto_msgTypes[7].OneofWrappers = []interface{}{
		(*ColumnResponse_StringVal)(nil),
		(*ColumnResponse_Uint64Val)(nil),
		(*ColumnResponse_Int64Val)(nil),
		(*ColumnResponse_BoolVal)(nil),
		(*ColumnResponse_BlobVal)(nil),
		(*ColumnResponse_Uint64ArrayVal)(nil),
		(*ColumnResponse_StringArrayVal)(nil),
		(*ColumnResponse_Float64Val)(nil),
		(*ColumnResponse_DecimalVal)(nil),
		(*ColumnResponse_TimestampVal)(nil),
	}
	file_ql_proto_msgTypes[11].OneofWrappers = []interface{}{
		(*IdsOrKeys_Ids)(nil),
		(*IdsOrKeys_Keys)(nil),
	}
	type x struct{}
	out := protoimpl.TypeBuilder{
		File: protoimpl.DescBuilder{
			GoPackagePath: reflect.TypeOf(x{}).PkgPath(),
			RawDescriptor: file_ql_proto_rawDesc,
			NumEnums:      0,
			NumMessages:   12,
			NumExtensions: 0,
			NumServices:   0,
		},
		GoTypes:           file_ql_proto_goTypes,
		DependencyIndexes: file_ql_proto_depIdxs,
		MessageInfos:      file_ql_proto_msgTypes,
	}.Build()
	File_ql_proto = out.File
	file_ql_proto_rawDesc = nil
	file_ql_proto_goTypes = nil
	file_ql_proto_depIdxs = nil
}
