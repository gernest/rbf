package core

import (
	"fmt"
	"time"

	"github.com/gernest/rbf/ql/pql"
	"github.com/gernest/rbf/quantum"
)

const (
	BaseTypeBool        = "bool"       //
	BaseTypeDecimal     = "decimal"    //
	BaseTypeID          = "id"         // non-keyed mutex
	BaseTypeIDSet       = "idset"      // non-keyed set
	BaseTypeIDSetQ      = "idsetq"     // non-keyed set timequantum
	BaseTypeInt         = "int"        //
	BaseTypeString      = "string"     // keyed mutex
	BaseTypeStringSet   = "stringset"  // keyed set
	BaseTypeStringSetQ  = "stringsetq" // keyed set timequantum
	BaseTypeTimestamp   = "timestamp"  //
	PrimaryKeyFieldName = FieldName("_id")
)

// Table represents a table and its configuration.
type Table struct {
	ID         TableID   `json:"id,omitempty"`
	Name       TableName `json:"name,omitempty"`
	Fields     []*Field  `json:"fields"`
	PartitionN int       `json:"partitionN"`

	Description string `json:"description,omitempty"`
	Owner       string `json:"owner,omitempty"`
	UpdatedBy   string `json:"updatedBy,omitempty"`
}

func (t *Table) Key() TableKey {
	return TableKey(t.ID)
}

// IsPrimaryKey returns true if the field is the primary key field (of either
// type ID or STRING).
func (f *Field) IsPrimaryKey() bool {
	return f.Name == PrimaryKeyFieldName
}

// StringKeys returns true if the table's primary key is either a string or a
// concatenation of fields.
func (t *Table) StringKeys() bool {
	for _, fld := range t.Fields {
		if fld.IsPrimaryKey() {
			if fld.Type == BaseTypeString {
				return true
			}
			break
		}
	}
	return false
}

// HasValidPrimaryKey returns false if the table does not contain a primary key
// field (which is required), or if the primary key field is not a valid type.
func (t *Table) HasValidPrimaryKey() bool {
	for _, fld := range t.Fields {
		if !fld.IsPrimaryKey() {
			continue
		}

		if fld.Type == BaseTypeID || fld.Type == BaseTypeString {
			return true
		}
	}
	return false
}

// FieldNames returns the list of field names associated with the table.
func (t *Table) FieldNames() []FieldName {
	ret := make([]FieldName, 0, len(t.Fields))
	for _, f := range t.Fields {
		ret = append(ret, f.Name)
	}
	return ret
}

// Field returns the field with the provided name. If a field with that name
// does not exist, the returned boolean will be false.
func (t *Table) Field(name FieldName) (*Field, bool) {
	for _, fld := range t.Fields {
		if fld.Name == name {
			return fld, true
		}
	}
	return nil, false
}

// TableKeyer is an interface implemented by any type which can produce, and be
// represented by, a TableKey. In the case of a QualifiedTable, its TableKey
// might be something like `tbl__org__db__tableid`, while a general pilosa
// implemenation might represent a table as a basic table name `foo`.
type TableKeyer interface {
	Key() TableKey
}

// StringTableKeyer is a helper type which can wrap a string, making it a
// TableKeyer. This is useful for certain calls to Execute() which take a string
// index name.
type StringTableKeyer string

func (s StringTableKeyer) Key() TableKey {
	return TableKey(s)
}

// TableKey is a globally unique identifier for a table; it is effectively the
// compound key: (org, database, table). This is (hopefully) the value that will
// be used when interfacing with services which are unaware of table qualifiers.
// For example, the FeatureBase server has no notion of organization or
// database; its top level type is index/indexName/table. So in this case, until
// and unless we introduce table qualifiers into FeatureBase, we will use
// TableKey as the value for index.Name.
type TableKey string

func (tk TableKey) Key() TableKey { return tk }

type TableID string

type TableName string

// FieldName is a typed string used for field names.
type FieldName string

// BaseType is a typed string used for field types.
type BaseType string

// Field represents a field and its configuration.
type Field struct {
	Name    FieldName    `json:"name"`
	Type    BaseType     `json:"type"`
	Options FieldOptions `json:"options"`
}

// FieldOptions represents options to set when initializing a field.
type FieldOptions struct {
	Min            pql.Decimal         `json:"min,omitempty"`
	Max            pql.Decimal         `json:"max,omitempty"`
	Scale          int64               `json:"scale,omitempty"`
	NoStandardView bool                `json:"no-standard-view,omitempty"` // TODO: we should remove this
	CacheType      string              `json:"cache-type,omitempty"`
	CacheSize      uint32              `json:"cache-size,omitempty"`
	TimeUnit       string              `json:"time-unit,omitempty"`
	Epoch          time.Time           `json:"epoch,omitempty"`
	TimeQuantum    quantum.TimeQuantum `json:"time-quantum,omitempty"`
	TTL            time.Duration       `json:"ttl,omitempty"`
	ForeignIndex   string              `json:"foreign-index,omitempty"`
	TrackExistence bool                `json:"track-existence"`
}

// Constants related to timestamp.
const (
	TimeUnitSeconds      = "s"
	TimeUnitMilliseconds = "ms"
	TimeUnitMicroseconds = "µs"
	TimeUnitUSeconds     = "us"
	TimeUnitNanoseconds  = "ns"
)

// IsValidTimeUnit returns true if unit is valid.
func IsValidTimeUnit(unit string) bool {
	switch unit {
	case TimeUnitSeconds, TimeUnitMilliseconds, TimeUnitMicroseconds, TimeUnitUSeconds, TimeUnitNanoseconds:
		return true
	default:
		return false
	}
}

// TimeUnitNanos returns the number of nanoseconds in unit.
func TimeUnitNanos(unit string) int64 {
	switch unit {
	case TimeUnitSeconds:
		return int64(time.Second)
	case TimeUnitMilliseconds:
		return int64(time.Millisecond)
	case TimeUnitMicroseconds, TimeUnitUSeconds:
		return int64(time.Microsecond)
	default:
		return int64(time.Nanosecond)
	}
}

// CheckEpochOutOfRange checks if the epoch is after max or before min
func CheckEpochOutOfRange(epoch, min, max time.Time) error {
	if epoch.After(max) || epoch.Before(min) {
		return fmt.Errorf("custom epoch too far from Unix epoch: %s", epoch)
	}
	return nil
}

// ValToTimestamp takes a timeunit and an integer value and converts it to time.Time
func ValToTimestamp(unit string, val int64) (time.Time, error) {
	switch unit {
	case TimeUnitSeconds:
		return time.Unix(val, 0).UTC(), nil
	case TimeUnitMilliseconds:
		return time.UnixMilli(val).UTC(), nil
	case TimeUnitMicroseconds, TimeUnitUSeconds:
		return time.UnixMicro(val).UTC(), nil
	case TimeUnitNanoseconds:
		return time.Unix(0, val).UTC(), nil
	default:
		return time.Time{}, fmt.Errorf("unknown time unit: '%v'", unit)
	}
}

// TimestampToVal takes a time unit and a time.Time and converts it to an integer value
func TimestampToVal(unit string, ts time.Time) int64 {
	switch unit {
	case TimeUnitSeconds:
		return ts.Unix()
	case TimeUnitMilliseconds:
		return ts.UnixMilli()
	case TimeUnitMicroseconds, TimeUnitUSeconds:
		return ts.UnixMicro()
	case TimeUnitNanoseconds:
		return ts.UnixNano()
	}
	return 0
}
