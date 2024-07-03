package core

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gernest/rbf/proto"
	"github.com/gernest/rbf/ql/pql"
)

type ExtractedTableField struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

type KeyOrID struct {
	ID    uint64
	Key   string
	Keyed bool
}

func (kid KeyOrID) MarshalJSON() ([]byte, error) {
	if kid.Keyed {
		return json.Marshal(kid.Key)
	}

	return json.Marshal(kid.ID)
}

type ExtractedTableColumn struct {
	Column KeyOrID       `json:"column"`
	Rows   []interface{} `json:"rows"`
}

type ExtractedTable struct {
	Fields  []ExtractedTableField  `json:"fields"`
	Columns []ExtractedTableColumn `json:"columns"`
}

// ToRows implements the ToRowser interface.
func (t ExtractedTable) ToRows(callback func(*proto.RowResponse) error) error {
	if len(t.Columns) == 0 {
		return nil
	}

	headers := make([]*proto.ColumnInfo, len(t.Fields)+1)
	colType := "uint64"
	if t.Columns[0].Column.Keyed {
		colType = "string"
	}
	headers[0] = &proto.ColumnInfo{
		Name:     "_id",
		Datatype: colType,
	}
	dataHeaders := headers[1:]
	for i, f := range t.Fields {
		dataHeaders[i] = &proto.ColumnInfo{
			Name:     f.Name,
			Datatype: f.Type,
		}
	}

	for _, c := range t.Columns {
		cols := make([]*proto.ColumnResponse, len(c.Rows)+1)
		if c.Column.Keyed {
			cols[0] = &proto.ColumnResponse{
				ColumnVal: &proto.ColumnResponse_StringVal{
					StringVal: c.Column.Key,
				},
			}
		} else {
			cols[0] = &proto.ColumnResponse{
				ColumnVal: &proto.ColumnResponse_Uint64Val{
					Uint64Val: c.Column.ID,
				},
			}
		}
		valCols := cols[1:]
		for i, r := range c.Rows {
			var col *proto.ColumnResponse
			switch r := r.(type) {
			case nil:
				col = &proto.ColumnResponse{}
			case bool:
				col = &proto.ColumnResponse{
					ColumnVal: &proto.ColumnResponse_BoolVal{
						BoolVal: r,
					},
				}
			case int64:
				col = &proto.ColumnResponse{
					ColumnVal: &proto.ColumnResponse_Int64Val{
						Int64Val: r,
					},
				}
			case uint64:
				col = &proto.ColumnResponse{
					ColumnVal: &proto.ColumnResponse_Uint64Val{
						Uint64Val: r,
					},
				}
			case string:
				col = &proto.ColumnResponse{
					ColumnVal: &proto.ColumnResponse_StringVal{
						StringVal: r,
					},
				}
			case []uint64:
				col = &proto.ColumnResponse{
					ColumnVal: &proto.ColumnResponse_Uint64ArrayVal{
						Uint64ArrayVal: &proto.Uint64Array{
							Vals: r,
						},
					},
				}
			case []string:
				col = &proto.ColumnResponse{
					ColumnVal: &proto.ColumnResponse_StringArrayVal{
						StringArrayVal: &proto.StringArray{
							Vals: r,
						},
					},
				}
			case pql.Decimal:
				rValue := r.Value()
				rValuePtr := &rValue
				col = &proto.ColumnResponse{
					ColumnVal: &proto.ColumnResponse_DecimalVal{
						DecimalVal: &proto.Decimal{
							Value: rValuePtr.Int64(),
							Scale: r.Scale,
						},
					},
				}
			case time.Time:
				col = &proto.ColumnResponse{
					ColumnVal: &proto.ColumnResponse_TimestampVal{
						TimestampVal: r.UTC().Format(time.RFC3339Nano),
					},
				}
			default:
				return fmt.Errorf("unsupported field value: %v (type: %T)", r, r)
			}
			valCols[i] = col
		}
		err := callback(&proto.RowResponse{
			Headers: headers,
			Columns: cols,
		})
		if err != nil {
			return err
		}
	}

	return nil
}

// ToTable converts the table to protobuf format.
func (t ExtractedTable) ToTable() (*proto.TableResponse, error) {
	return proto.RowsToTable(t, len(t.Columns))
}

type ExtractedIDColumn struct {
	ColumnID uint64
	Rows     [][]uint64
}

type ExtractedIDMatrix struct {
	Fields  []string
	Columns []ExtractedIDColumn
}

func (e *ExtractedIDMatrix) Append(m ExtractedIDMatrix) {
	e.Columns = append(e.Columns, m.Columns...)
	if e.Fields == nil {
		e.Fields = m.Fields
	}
}
