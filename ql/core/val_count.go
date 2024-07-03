package core

import (
	"fmt"
	"time"

	"github.com/gernest/rbf/proto"
	"github.com/gernest/rbf/ql/pql"
)

// ValCount represents a grouping of sum & count for Sum() and Average() calls. Also Min, Max....
type ValCount struct {
	Val          int64        `json:"value"`
	FloatVal     float64      `json:"floatValue"`
	DecimalVal   *pql.Decimal `json:"decimalValue"`
	TimestampVal time.Time    `json:"timestampValue"`
	Count        int64        `json:"count"`
}

func (v *ValCount) Clone() (r *ValCount) {
	r = &ValCount{
		Val:          v.Val,
		FloatVal:     v.FloatVal,
		TimestampVal: v.TimestampVal,
		Count:        v.Count,
	}
	if v.DecimalVal != nil {
		r.DecimalVal = v.DecimalVal.Clone()
	}
	return
}

// ToTable implements the ToTabler interface.
func (v ValCount) ToTable() (*proto.TableResponse, error) {
	return proto.RowsToTable(&v, 1)
}

// ToRows implements the ToRowser interface.
func (v ValCount) ToRows(callback func(*proto.RowResponse) error) error {
	var ci []*proto.ColumnInfo
	// ValCount can have a decimal, float, or integer value, but
	// not more than one (as of this writing).
	if v.DecimalVal != nil {
		ci = []*proto.ColumnInfo{
			{Name: "value", Datatype: "decimal"},
			{Name: "count", Datatype: "int64"},
		}
		vValue := v.DecimalVal.Value()
		vValuePtr := &vValue
		if err := callback(&proto.RowResponse{
			Headers: ci,
			Columns: []*proto.ColumnResponse{
				{ColumnVal: &proto.ColumnResponse_DecimalVal{DecimalVal: &proto.Decimal{Value: vValuePtr.Int64(), Scale: v.DecimalVal.Scale}}},
				{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: v.Count}},
			},
		}); err != nil {
			return fmt.Errorf("calling callback%w", err)
		}
	} else if v.FloatVal != 0 {
		ci = []*proto.ColumnInfo{
			{Name: "value", Datatype: "float64"},
			{Name: "count", Datatype: "int64"},
		}
		if err := callback(&proto.RowResponse{
			Headers: ci,
			Columns: []*proto.ColumnResponse{
				{ColumnVal: &proto.ColumnResponse_Float64Val{Float64Val: v.FloatVal}},
				{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: v.Count}},
			},
		}); err != nil {
			return fmt.Errorf("calling callback%w", err)
		}
	} else if !v.TimestampVal.IsZero() {
		ci = []*proto.ColumnInfo{
			{Name: "value", Datatype: "string"},
			{Name: "count", Datatype: "int64"},
		}
		if err := callback(&proto.RowResponse{
			Headers: ci,
			Columns: []*proto.ColumnResponse{
				{ColumnVal: &proto.ColumnResponse_StringVal{StringVal: v.TimestampVal.Format(time.RFC3339Nano)}},
				{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: v.Count}},
			},
		}); err != nil {
			return fmt.Errorf("calling callback%w", err)
		}
	} else {
		ci = []*proto.ColumnInfo{
			{Name: "value", Datatype: "int64"},
			{Name: "count", Datatype: "int64"},
		}
		if err := callback(&proto.RowResponse{
			Headers: ci,
			Columns: []*proto.ColumnResponse{
				{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: v.Val}},
				{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: v.Count}},
			},
		}); err != nil {
			return fmt.Errorf("calling callback%w", err)
		}
	}
	return nil
}

func (vc *ValCount) Add(other ValCount) ValCount {
	return ValCount{
		Val:   vc.Val + other.Val,
		Count: vc.Count + other.Count,
	}
}

// smaller returns the smaller of the two ValCounts.
func (vc *ValCount) Smaller(other ValCount) ValCount {
	if vc.DecimalVal != nil || other.DecimalVal != nil {
		return vc.decimalSmaller(other)
	} else if vc.FloatVal != 0 || other.FloatVal != 0 {
		return vc.floatSmaller(other)
	} else if !vc.TimestampVal.IsZero() || !other.TimestampVal.IsZero() {
		return vc.timestampSmaller(other)
	}
	if vc.Count == 0 || (other.Val < vc.Val && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.Val == other.Val {
		extra += other.Count
	}
	return ValCount{
		Val:          vc.Val,
		Count:        vc.Count + extra,
		DecimalVal:   vc.DecimalVal,
		FloatVal:     vc.FloatVal,
		TimestampVal: vc.TimestampVal,
	}
}

// timestampSmaller returns the smaller of the two (vc or other), while merging the count
// if they are equal.
func (vc *ValCount) timestampSmaller(other ValCount) ValCount {
	if other.TimestampVal.Equal(time.Time{}) {
		return *vc
	}
	if vc.Count == 0 || vc.TimestampVal.Equal(time.Time{}) || (other.TimestampVal.Before(vc.TimestampVal) && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.TimestampVal.Equal(other.TimestampVal) {
		extra += other.Count
	}
	return ValCount{
		Val:          vc.Val,
		TimestampVal: vc.TimestampVal,
		Count:        vc.Count + extra,
	}
}

// decimalSmaller returns the smaller of the two (vc or other), while merging the count
// if they are equal.
func (vc *ValCount) decimalSmaller(other ValCount) ValCount {
	if other.DecimalVal == nil {
		return *vc
	}
	if vc.Count == 0 || vc.DecimalVal == nil || (other.DecimalVal.LessThan(*vc.DecimalVal) && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.DecimalVal.EqualTo(*other.DecimalVal) {
		extra += other.Count
	}
	return ValCount{
		DecimalVal: vc.DecimalVal,
		Count:      vc.Count + extra,
	}
}

// floatSmaller returns the smaller of the two (vc or other), while merging the count
// if they are equal.
func (vc *ValCount) floatSmaller(other ValCount) ValCount {
	if vc.Count == 0 || (other.FloatVal < vc.FloatVal && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.FloatVal == other.FloatVal {
		extra += other.Count
	}
	return ValCount{
		FloatVal: vc.FloatVal,
		Count:    vc.Count + extra,
	}
}

// larger returns the larger of the two ValCounts.
func (vc *ValCount) Larger(other ValCount) ValCount {
	if vc.DecimalVal != nil || other.DecimalVal != nil {
		return vc.decimalLarger(other)
	} else if vc.FloatVal != 0 || other.FloatVal != 0 {
		return vc.floatLarger(other)
	} else if !vc.TimestampVal.Equal(time.Time{}) || !other.TimestampVal.Equal(time.Time{}) {
		return vc.timestampLarger(other)
	}
	if vc.Count == 0 || (other.Val > vc.Val && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.Val == other.Val {
		extra += other.Count
	}
	return ValCount{
		Val:          vc.Val,
		Count:        vc.Count + extra,
		DecimalVal:   vc.DecimalVal,
		FloatVal:     vc.FloatVal,
		TimestampVal: vc.TimestampVal,
	}
}

// timestampLarger returns the larger of the two (vc or other), while merging the count
// if they are equal.
func (vc *ValCount) timestampLarger(other ValCount) ValCount {
	if other.TimestampVal.Equal(time.Time{}) {
		return *vc
	}
	if vc.Count == 0 || vc.TimestampVal.Equal(time.Time{}) || (other.TimestampVal.After(vc.TimestampVal) && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.TimestampVal.Equal(other.TimestampVal) {
		extra += other.Count
	}
	return ValCount{
		Val:          vc.Val,
		TimestampVal: vc.TimestampVal,
		Count:        vc.Count + extra,
	}
}

// decimalLarger returns the larger of the two (vc or other), while merging the count
// if they are equal.
func (vc *ValCount) decimalLarger(other ValCount) ValCount {
	if other.DecimalVal == nil {
		return *vc
	}
	if vc.Count == 0 || vc.DecimalVal == nil || (other.DecimalVal.GreaterThan(*vc.DecimalVal) && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.DecimalVal.EqualTo(*other.DecimalVal) {
		extra += other.Count
	}
	return ValCount{
		DecimalVal: vc.DecimalVal,
		Count:      vc.Count + extra,
	}
}

// floatLarger returns the larger of the two (vc or other), while merging the count
// if they are equal.
func (vc *ValCount) floatLarger(other ValCount) ValCount {
	if vc.Count == 0 || (other.FloatVal > vc.FloatVal && other.Count > 0) {
		return other
	}
	extra := int64(0)
	if vc.FloatVal == other.FloatVal {
		extra += other.Count
	}
	return ValCount{
		FloatVal: vc.FloatVal,
		Count:    vc.Count + extra,
	}
}
