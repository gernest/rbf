package core

import (
	"fmt"

	"github.com/gernest/rbf/proto"
)

// RowIdentifiers is a return type for a list of
// row ids or row keys. The names `Rows` and `Keys`
// are meant to follow the same convention as the
// Row query which returns `Columns` and `Keys`.
// TODO: Rename this to something better. Anything.
type RowIdentifiers struct {
	Rows  []uint64 `json:"rows"`
	Keys  []string `json:"keys,omitempty"`
	Field string
}

func (r *RowIdentifiers) Clone() (clone *RowIdentifiers) {
	clone = &RowIdentifiers{
		Field: r.Field,
	}
	if r.Rows != nil {
		clone.Rows = make([]uint64, len(r.Rows))
		copy(clone.Rows, r.Rows)
	}
	if r.Keys != nil {
		clone.Keys = make([]string, len(r.Keys))
		copy(clone.Keys, r.Keys)
	}
	return
}

// ToTable implements the ToTabler interface.
func (r RowIdentifiers) ToTable() (*proto.TableResponse, error) {
	var n int
	if len(r.Keys) > 0 {
		n = len(r.Keys)
	} else {
		n = len(r.Rows)
	}
	return proto.RowsToTable(&r, n)
}

// ToRows implements the ToRowser interface.
func (r RowIdentifiers) ToRows(callback func(*proto.RowResponse) error) error {
	if len(r.Keys) > 0 {
		ci := []*proto.ColumnInfo{{Name: r.Field, Datatype: "string"}}
		for _, key := range r.Keys {
			if err := callback(&proto.RowResponse{
				Headers: ci,
				Columns: []*proto.ColumnResponse{
					{ColumnVal: &proto.ColumnResponse_StringVal{StringVal: key}},
				},
			}); err != nil {
				return fmt.Errorf("calling callback %w", err)
			}
			ci = nil
		}
	} else {
		ci := []*proto.ColumnInfo{{Name: r.Field, Datatype: "uint64"}}
		for _, id := range r.Rows {
			if err := callback(&proto.RowResponse{
				Headers: ci,
				Columns: []*proto.ColumnResponse{
					{ColumnVal: &proto.ColumnResponse_Uint64Val{Uint64Val: uint64(id)}},
				},
			}); err != nil {
				return fmt.Errorf("calling callback %w", err)
			}
			ci = nil
		}
	}
	return nil
}
