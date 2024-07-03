package core

import (
	"fmt"

	"github.com/gernest/rbf/proto"
)

type DistinctTimestamp struct {
	Values []string
	Name   string
}

var _ proto.ToRowser = DistinctTimestamp{}

// ToRows implements the ToRowser interface.
func (d DistinctTimestamp) ToRows(callback func(*proto.RowResponse) error) error {
	for _, ts := range d.Values {
		row := &proto.RowResponse{
			Headers: []*proto.ColumnInfo{
				{
					Name:     d.Name,
					Datatype: "timestamp",
				},
			},
			Columns: []*proto.ColumnResponse{
				{
					ColumnVal: &proto.ColumnResponse_TimestampVal{
						TimestampVal: ts,
					},
				},
			},
		}
		if err := callback(row); err != nil {
			return fmt.Errorf("calling callback %w", err)
		}
	}

	return nil
}

// ToTable implements the ToTabler interface for DistinctTimestamp
func (d DistinctTimestamp) ToTable() (*proto.TableResponse, error) {
	return proto.RowsToTable(&d, len(d.Values))
}

// Union returns the union of the values of `d` and `other`
func (d *DistinctTimestamp) Union(other DistinctTimestamp) DistinctTimestamp {
	both := map[string]struct{}{}
	for _, val := range d.Values {
		both[val] = struct{}{}
	}
	for _, val := range other.Values {
		both[val] = struct{}{}
	}
	vals := []string{}
	for key := range both {
		vals = append(vals, key)
	}
	return DistinctTimestamp{Name: d.Name, Values: vals}
}
