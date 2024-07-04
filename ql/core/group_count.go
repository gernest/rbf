package core

import (
	"encoding/json"
	"fmt"
	"time"
	"unsafe"

	"github.com/gernest/rbf/proto"
	"github.com/gernest/rbf/ql/pql"
	"github.com/pkg/errors"
)

// FieldRow is used to distinguish rows in a group by result.
type FieldRow struct {
	Field  Field  `json:"field"`
	RowID  uint64 `json:"rowID"`
	RowKey string `json:"rowKey,omitempty"`
	Value  *int64 `json:"value,omitempty"`
}

func (fr *FieldRow) Clone() (clone *FieldRow) {
	clone = &FieldRow{
		Field:  fr.Field,
		RowID:  fr.RowID,
		RowKey: fr.RowKey,
	}
	if fr.Value != nil {
		// deep copy, for safety.
		v := *fr.Value
		clone.Value = &v
	}

	return
}

// MarshalJSON marshals FieldRow to JSON such that
// either a Key or an ID is included.
func (fr FieldRow) MarshalJSON() ([]byte, error) {
	if fr.Value != nil {
		if fr.Field.Type == BaseTypeTimestamp {
			ts, err := ValToTimestamp(fr.Field.Options.TimeUnit, int64(*fr.Value)+fr.Field.Options.Scale)
			if err != nil {
				return nil, errors.Wrap(err, "translating value to timestamp")
			}
			return json.Marshal(struct {
				Field string `json:"field"`
				Value string `json:"value"`
			}{
				Field: string(fr.Field.Name),
				Value: ts.Format(time.RFC3339Nano),
			})
		} else {
			return json.Marshal(struct {
				Field string `json:"field"`
				Value int64  `json:"value"`
			}{
				Field: string(fr.Field.Name),
				Value: *fr.Value,
			})
		}
	}

	if fr.RowKey != "" {
		return json.Marshal(struct {
			Field  string `json:"field"`
			RowKey string `json:"rowKey"`
		}{
			Field:  string(fr.Field.Name),
			RowKey: fr.RowKey,
		})
	}

	return json.Marshal(struct {
		Field string `json:"field"`
		RowID uint64 `json:"rowID"`
	}{
		Field: string(fr.Field.Name),
		RowID: fr.RowID,
	})
}

// String is the FieldRow stringer.
func (fr FieldRow) String() string {
	if fr.Value != nil {
		return fmt.Sprintf("%s.%d.%d.%s", fr.Field.Name, fr.RowID, *fr.Value, fr.RowKey)
	}
	return fmt.Sprintf("%s.%d.%s", fr.Field.Name, fr.RowID, fr.RowKey)
}

type aggregateType int

const (
	nilAggregate        aggregateType = 0
	sumAggregate        aggregateType = 1
	distinctAggregate   aggregateType = 2
	decimalSumAggregate aggregateType = 3
)

// GroupCounts is a list of GroupCount.
type GroupCounts struct {
	groups        []GroupCount
	aggregateType aggregateType
}

// AggregateColumn gives the likely column name to use for aggregates, because
// for historical reasons we used "sum" when it was a sum, but don't want to
// use that when it's something else. This will likely get revisited.
func (g *GroupCounts) AggregateColumn() string {
	switch g.aggregateType {
	case sumAggregate:
		return "sum"
	case distinctAggregate:
		return "aggregate"
	case decimalSumAggregate:
		return "decimalSum"
	default:
		return ""
	}
}

// Groups is a convenience method to let us not worry as much about the
// potentially-nil nature of a *GroupCounts.
func (g *GroupCounts) Groups() []GroupCount {
	if g == nil {
		return nil
	}
	return g.groups
}

// NewGroupCounts creates a GroupCounts with the given type and slice
// of GroupCount objects. There's intentionally no externally-accessible way
// to change the []GroupCount after creation.
func NewGroupCounts(agg string, groups ...GroupCount) *GroupCounts {
	var aggType aggregateType
	switch agg {
	case "sum":
		aggType = sumAggregate
	case "aggregate":
		aggType = distinctAggregate
	case "decimalSum":
		aggType = decimalSumAggregate
	case "":
		aggType = nilAggregate
	default:
		panic(fmt.Sprintf("invalid aggregate type %q", agg))
	}
	return &GroupCounts{aggregateType: aggType, groups: groups}
}

// ToTable implements the ToTabler interface.
func (g *GroupCounts) ToTable() (*proto.TableResponse, error) {
	return proto.RowsToTable(g, len(g.Groups()))
}

// ToRows implements the ToRowser interface.
func (g *GroupCounts) ToRows(callback func(*proto.RowResponse) error) error {
	agg := g.AggregateColumn()
	for i, gc := range g.Groups() {
		var ci []*proto.ColumnInfo
		if i == 0 {
			for _, fieldRow := range gc.Group {
				if fieldRow.RowKey != "" {
					ci = append(ci, &proto.ColumnInfo{Name: string(fieldRow.Field.Name), Datatype: "string"})
				} else if fieldRow.Value != nil {
					ci = append(ci, &proto.ColumnInfo{Name: string(fieldRow.Field.Name), Datatype: "int64"})
				} else {
					ci = append(ci, &proto.ColumnInfo{Name: string(fieldRow.Field.Name), Datatype: "uint64"})
				}
			}
			ci = append(ci, &proto.ColumnInfo{Name: "count", Datatype: "uint64"})
			if agg != "" {
				ci = append(ci, &proto.ColumnInfo{Name: agg, Datatype: "int64"})
			}

		}
		rowResp := &proto.RowResponse{
			Headers: ci,
			Columns: []*proto.ColumnResponse{},
		}

		for _, fieldRow := range gc.Group {
			if fieldRow.RowKey != "" {
				rowResp.Columns = append(rowResp.Columns, &proto.ColumnResponse{ColumnVal: &proto.ColumnResponse_StringVal{StringVal: fieldRow.RowKey}})
			} else if fieldRow.Value != nil {
				rowResp.Columns = append(rowResp.Columns, &proto.ColumnResponse{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: *fieldRow.Value}})
			} else {
				rowResp.Columns = append(rowResp.Columns, &proto.ColumnResponse{ColumnVal: &proto.ColumnResponse_Uint64Val{Uint64Val: fieldRow.RowID}})
			}
		}
		rowResp.Columns = append(rowResp.Columns,
			&proto.ColumnResponse{ColumnVal: &proto.ColumnResponse_Uint64Val{Uint64Val: gc.Count}})
		if agg != "" {
			rowResp.Columns = append(rowResp.Columns,
				&proto.ColumnResponse{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: gc.Agg}})
		}
		if err := callback(rowResp); err != nil {
			return errors.Wrap(err, "calling callback")
		}
	}
	return nil
}

// MarshalJSON makes GroupCounts satisfy interface json.Marshaler and
// customizes the JSON output of the aggregate field label.
func (g *GroupCounts) MarshalJSON() ([]byte, error) {
	groups := g.Groups()
	var counts interface{} = groups

	if len(groups) == 0 {
		return []byte("[]"), nil
	}

	switch g.aggregateType {
	case sumAggregate:
		counts = *(*[]groupCountSum)(unsafe.Pointer(&groups))
	case distinctAggregate:
		counts = *(*[]groupCountAggregate)(unsafe.Pointer(&groups))
	case decimalSumAggregate:
		counts = *(*[]groupCountDecimalSum)(unsafe.Pointer(&groups))
	}
	return json.Marshal(counts)
}

// GroupCount represents a result item for a group by query.
type GroupCount struct {
	Group      []FieldRow   `json:"group"`
	Count      uint64       `json:"count"`
	Agg        int64        `json:"-"`
	DecimalAgg *pql.Decimal `json:"-"`
}

type groupCountSum struct {
	Group      []FieldRow   `json:"group"`
	Count      uint64       `json:"count"`
	Agg        int64        `json:"sum"`
	DecimalAgg *pql.Decimal `json:"-"`
}

type groupCountAggregate struct {
	Group      []FieldRow   `json:"group"`
	Count      uint64       `json:"count"`
	Agg        int64        `json:"aggregate"`
	DecimalAgg *pql.Decimal `json:"-"`
}

type groupCountDecimalSum struct {
	Group      []FieldRow   `json:"group"`
	Count      uint64       `json:"count"`
	Agg        int64        `json:"-"`
	DecimalAgg *pql.Decimal `json:"sum"`
}

var (
	_ GroupCount = GroupCount(groupCountSum{})
	_ GroupCount = GroupCount(groupCountAggregate{})
	_ GroupCount = GroupCount(groupCountDecimalSum{})
)

func (g *GroupCount) Clone() (r *GroupCount) {
	r = &GroupCount{
		Group:      make([]FieldRow, len(g.Group)),
		Count:      g.Count,
		Agg:        g.Agg,
		DecimalAgg: g.DecimalAgg,
	}
	for i := range g.Group {
		r.Group[i] = *(g.Group[i].Clone())
	}
	return
}

// mergeGroupCounts merges two slices of GroupCounts throwing away any that go
// beyond the limit. It assume that the two slices are sorted by the row ids in
// the fields of the group counts. It may modify its arguments.
func mergeGroupCounts(a, b []GroupCount, limit int) []GroupCount {
	if limit > len(a)+len(b) {
		limit = len(a) + len(b)
	}
	ret := make([]GroupCount, 0, limit)
	i, j := 0, 0
	for i < len(a) && j < len(b) && len(ret) < limit {
		switch a[i].Compare(b[j]) {
		case -1:
			ret = append(ret, a[i])
			i++
		case 0:
			a[i].Count += b[j].Count
			a[i].Agg += b[j].Agg
			if a[i].DecimalAgg != nil && b[j].DecimalAgg != nil {
				sum := pql.AddDecimal(*a[i].DecimalAgg, *b[j].DecimalAgg)
				a[i].DecimalAgg = &sum
			}
			ret = append(ret, a[i])
			i++
			j++
		case 1:
			ret = append(ret, b[j])
			j++
		}
	}
	for ; i < len(a) && len(ret) < limit; i++ {
		ret = append(ret, a[i])
	}
	for ; j < len(b) && len(ret) < limit; j++ {
		ret = append(ret, b[j])
	}
	return ret
}

// Compare is used in ordering two GroupCount objects.
func (g GroupCount) Compare(o GroupCount) int {
	for i, g1 := range g.Group {
		g2 := o.Group[i]

		if g1.Value != nil && g2.Value != nil {
			if *g1.Value < *g2.Value {
				return -1
			}
			if *g1.Value > *g2.Value {
				return 1
			}
		} else {
			if g1.RowID < g2.RowID {
				return -1
			}
			if g1.RowID > g2.RowID {
				return 1
			}
		}
	}
	return 0
}

func (g GroupCount) satisfiesCondition(subj string, cond *pql.Condition) bool {
	switch subj {
	case "count":
		switch cond.Op {
		case pql.EQ, pql.NEQ, pql.LT, pql.LTE, pql.GT, pql.GTE:
			val, ok := cond.Uint64Value()
			if !ok {
				return false
			}
			if cond.Op == pql.EQ {
				if g.Count == val {
					return true
				}
			} else if cond.Op == pql.NEQ {
				if g.Count != val {
					return true
				}
			} else if cond.Op == pql.LT {
				if g.Count < val {
					return true
				}
			} else if cond.Op == pql.LTE {
				if g.Count <= val {
					return true
				}
			} else if cond.Op == pql.GT {
				if g.Count > val {
					return true
				}
			} else if cond.Op == pql.GTE {
				if g.Count >= val {
					return true
				}
			}
		case pql.BETWEEN, pql.BTWN_LT_LTE, pql.BTWN_LTE_LT, pql.BTWN_LT_LT:
			val, ok := cond.Uint64SliceValue()
			if !ok {
				return false
			}
			if cond.Op == pql.BETWEEN {
				if val[0] <= g.Count && g.Count <= val[1] {
					return true
				}
			} else if cond.Op == pql.BTWN_LT_LTE {
				if val[0] < g.Count && g.Count <= val[1] {
					return true
				}
			} else if cond.Op == pql.BTWN_LTE_LT {
				if val[0] <= g.Count && g.Count < val[1] {
					return true
				}
			} else if cond.Op == pql.BTWN_LT_LT {
				if val[0] < g.Count && g.Count < val[1] {
					return true
				}
			}
		}
	case "sum":
		switch cond.Op {
		case pql.EQ, pql.NEQ, pql.LT, pql.LTE, pql.GT, pql.GTE:
			val, ok := cond.Int64Value()
			if !ok {
				return false
			}
			if cond.Op == pql.EQ {
				if g.Agg == val {
					return true
				}
			} else if cond.Op == pql.NEQ {
				if g.Agg != val {
					return true
				}
			} else if cond.Op == pql.LT {
				if g.Agg < val {
					return true
				}
			} else if cond.Op == pql.LTE {
				if g.Agg <= val {
					return true
				}
			} else if cond.Op == pql.GT {
				if g.Agg > val {
					return true
				}
			} else if cond.Op == pql.GTE {
				if g.Agg >= val {
					return true
				}
			}
		case pql.BETWEEN, pql.BTWN_LT_LTE, pql.BTWN_LTE_LT, pql.BTWN_LT_LT:
			val, ok := cond.Int64SliceValue()
			if !ok {
				return false
			}
			if cond.Op == pql.BETWEEN {
				if val[0] <= g.Agg && g.Agg <= val[1] {
					return true
				}
			} else if cond.Op == pql.BTWN_LT_LTE {
				if val[0] < g.Agg && g.Agg <= val[1] {
					return true
				}
			} else if cond.Op == pql.BTWN_LTE_LT {
				if val[0] <= g.Agg && g.Agg < val[1] {
					return true
				}
			} else if cond.Op == pql.BTWN_LT_LT {
				if val[0] < g.Agg && g.Agg < val[1] {
					return true
				}
			}
		}
	}
	return false
}

// ApplyConditionToGroupCounts filters the contents of gcs according
// to the condition. Currently, `count` and `sum` are the only
// fields supported.
func ApplyConditionToGroupCounts(gcs []GroupCount, subj string, cond *pql.Condition) []GroupCount {
	var i int
	for _, gc := range gcs {
		if !gc.satisfiesCondition(subj, cond) {
			continue // drop this GroupCount
		}
		gcs[i] = gc
		i++
	}
	return gcs[:i]
}
