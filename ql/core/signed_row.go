package core

import (
	"fmt"

	"github.com/gernest/rbf/proto"
	"github.com/gernest/rows"
)

// SignedRow represents a signed *Row with two (neg/pos) *Rows.
type SignedRow struct {
	Neg   *rows.Row `json:"neg"`
	Pos   *rows.Row `json:"pos"`
	Field string    `json:"-"`
}

func (s *SignedRow) Clone() (r *SignedRow) {
	r = &SignedRow{
		Neg:   s.Neg.Clone(), // Row.Clone() returns nil for nil.
		Pos:   s.Pos.Clone(),
		Field: s.Field,
	}
	return
}

// ToTable implements the ToTabler interface.
func (s SignedRow) ToTable() (*proto.TableResponse, error) {
	var n uint64
	if s.Neg != nil {
		n += s.Neg.Count()
	}
	if s.Pos != nil {
		n += s.Pos.Count()
	}
	return proto.RowsToTable(&s, int(n))
}

// ToRows implements the ToRowser interface.
func (s SignedRow) ToRows(callback func(*proto.RowResponse) error) error {
	ci := []*proto.ColumnInfo{{Name: s.Field, Datatype: "int64"}}
	if s.Neg != nil {
		negs := s.Neg.Columns()
		for i := len(negs) - 1; i >= 0; i-- {
			val, err := toNegInt64(negs[i])
			if err != nil {
				return fmt.Errorf("converting uint64 to int64 (negative) %w", err)
			}

			if err := callback(&proto.RowResponse{
				Headers: ci,
				Columns: []*proto.ColumnResponse{
					{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: val}},
				},
			}); err != nil {
				return fmt.Errorf("calling callback %w", err)
			}
			ci = nil
		}
	}
	if s.Pos != nil {
		for _, id := range s.Pos.Columns() {
			val, err := toInt64(id)
			if err != nil {
				return fmt.Errorf("converting uint64 to int64 (positive) %w", err)
			}

			if err := callback(&proto.RowResponse{
				Headers: ci,
				Columns: []*proto.ColumnResponse{
					{ColumnVal: &proto.ColumnResponse_Int64Val{Int64Val: val}},
				},
			}); err != nil {
				return fmt.Errorf("calling callback %w", err)
			}
			ci = nil
		}
	}
	return nil
}

func toNegInt64(n uint64) (int64, error) {
	const absMinInt64 = uint64(1 << 63)

	if n > absMinInt64 {
		return 0, fmt.Errorf("value %d overflows int64", n)
	}

	if n == absMinInt64 {
		return int64(-1 << 63), nil
	}

	// n < 1 << 63
	return -int64(n), nil
}

func toInt64(n uint64) (int64, error) {
	const maxInt64 = uint64(1<<63) - 1

	if n > maxInt64 {
		return 0, fmt.Errorf("value %d overflows int64", n)
	}

	return int64(n), nil
}

func (sr *SignedRow) Union(other SignedRow) SignedRow {
	ret := SignedRow{rows.NewRow(), rows.NewRow(), ""}

	// merge in sr
	if sr != nil {
		if sr.Neg != nil {
			ret.Neg = ret.Neg.Union(sr.Neg)
		}
		if sr.Pos != nil {
			ret.Pos = ret.Pos.Union(sr.Pos)
		}
	}

	// merge in other
	if other.Neg != nil {
		ret.Neg = ret.Neg.Union(other.Neg)
	}
	if other.Pos != nil {
		ret.Pos = ret.Pos.Union(other.Pos)
	}

	return ret
}
