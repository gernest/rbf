// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"
	"strings"

	"github.com/gernest/rbf/ql/core"
	"github.com/gernest/rbf/ql/pql"
	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

type TableQueryHint struct {
	name   string
	params []string
}

// PlanOpPQLTableScan plan operator handles a PQL table scan
type PlanOpPQLTableScan struct {
	planner            *ExecutionPlanner
	tableName          string
	columns            []string
	filter             types.PlanExpression
	timeQuantumFilters []types.PlanExpression
	topExpr            types.PlanExpression
	hints              []*TableQueryHint
	warnings           []string
}

func NewPlanOpPQLTableScan(p *ExecutionPlanner, tableName string, columns []string, hints []*TableQueryHint) *PlanOpPQLTableScan {
	return &PlanOpPQLTableScan{
		planner:            p,
		tableName:          tableName,
		columns:            columns,
		timeQuantumFilters: make([]types.PlanExpression, 0),
		hints:              hints,
		warnings:           make([]string, 0),
	}
}

func (p *PlanOpPQLTableScan) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
	result["tableName"] = p.tableName
	if p.topExpr != nil {
		result["topExpr"] = p.topExpr.Plan()
	}
	if p.filter != nil {
		result["filter"] = p.filter.Plan()
	}
	tqfilters := make([]map[string]interface{}, len(p.timeQuantumFilters))
	for i, f := range p.timeQuantumFilters {
		tqfilters[i] = f.Plan()
	}
	result["tqfilters"] = tqfilters
	result["columns"] = p.columns
	return result
}

func (p *PlanOpPQLTableScan) String() string {
	return ""
}

func (p *PlanOpPQLTableScan) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpPQLTableScan) Warnings() []string {
	return p.warnings
}

func (p *PlanOpPQLTableScan) Name() string {
	return p.tableName
}

func (p *PlanOpPQLTableScan) IsFilterable() bool {
	return true
}

func (p *PlanOpPQLTableScan) UpdateFilters(filterCondition types.PlanExpression) (types.PlanOperator, error) {
	p.filter = filterCondition
	return p, nil
}

func (p *PlanOpPQLTableScan) UpdateTimeQuantumFilters(filters ...types.PlanExpression) (types.PlanOperator, error) {
	p.timeQuantumFilters = filters
	return p, nil
}

func (p *PlanOpPQLTableScan) Schema() types.Schema {
	result := make(types.Schema, 0)

	tname := core.TableName(p.tableName)
	table, err := p.planner.schemaAPI.TableByName(context.Background(), tname)
	if err != nil {
		return result
	}

	for _, col := range p.columns {
		for _, fld := range table.Fields {
			if strings.EqualFold(string(fld.Name), col) {
				result = append(result, &types.PlannerColumn{
					ColumnName:   string(fld.Name),
					RelationName: p.tableName,
					Type:         fieldSQLDataType(fld),
				})
				break
			}
		}
	}
	return result
}

func (p *PlanOpPQLTableScan) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpPQLTableScan) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &tableScanRowIter{
		planner:            p.planner,
		tableName:          p.tableName,
		columns:            p.columns,
		predicate:          p.filter,
		timeQuantumFilters: p.timeQuantumFilters,
		topExpr:            p.topExpr,
	}, nil
}

func (p *PlanOpPQLTableScan) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return nil, nil
}

func (p *PlanOpPQLTableScan) PrimaryKeyType() (parser.ExprDataType, error) {
	tname := core.TableName(p.tableName)
	table, err := p.planner.schemaAPI.TableByName(context.Background(), tname)
	if err != nil {
		return nil, err
	}

	if table.StringKeys() {
		return parser.NewDataTypeString(), nil
	}
	return parser.NewDataTypeID(), nil
}

type targetColumn struct {
	columnIdx    int
	srcColumnIdx int
	columnName   string
	dataType     parser.ExprDataType
}

type tableScanRowIter struct {
	planner            *ExecutionPlanner
	tableName          string
	columns            []string
	predicate          types.PlanExpression
	timeQuantumFilters []types.PlanExpression
	topExpr            types.PlanExpression

	result    []core.ExtractedTableColumn
	rowWidth  int
	columnMap map[string]*targetColumn
}

var _ types.RowIterator = (*tableScanRowIter)(nil)

func (i *tableScanRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.result == nil {
		err := i.planner.checkAccess(ctx, i.tableName, accessTypeReadData)
		if err != nil {
			return nil, err
		}

		//go get the schema def and map names to indexes in the resultant row
		tname := core.TableName(i.tableName)
		table, err := i.planner.schemaAPI.TableByName(ctx, tname)
		if err != nil {
			if isTableNotFoundError(err) {
				return nil, sql3.NewErrInternalf("table not found '%s'", i.tableName)
			}
			return nil, err
		}
		i.rowWidth = len(i.columns)

		i.columnMap = make(map[string]*targetColumn)
		for idx, col := range i.columns {
			for _, fld := range table.Fields {
				if strings.EqualFold(col, string(fld.Name)) {
					i.columnMap[string(fld.Name)] = &targetColumn{
						columnIdx:    idx,
						srcColumnIdx: -1,
						columnName:   string(fld.Name),
						dataType:     fieldSQLDataType(fld),
					}
					break
				}
			}
		}

		var cond *pql.Call

		cond, err = i.planner.generatePQLCallFromExpr(ctx, i.predicate)
		if err != nil {
			return nil, err
		}
		if cond == nil {
			cond = &pql.Call{Name: "All"}
		}

		if i.topExpr != nil {
			_, ok := i.topExpr.(*intLiteralPlanExpression)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected top expression type: %T", i.topExpr)
			}
			pqlValue, err := planExprToValue(i.topExpr)
			if err != nil {
				return nil, err
			}
			cond = &pql.Call{
				Name:     "Limit",
				Children: []*pql.Call{cond},
				Args:     map[string]interface{}{"limit": pqlValue},
				Type:     pql.PrecallGlobal,
			}
		}

		call := &pql.Call{Name: "Extract", Children: []*pql.Call{cond}}
		for _, c := range i.columns {

			// skip the _id field
			if strings.EqualFold(c, string(core.PrimaryKeyFieldName)) {
				continue
			}

			foundInTimeQuantumFilters := false
			for _, tqf := range i.timeQuantumFilters {
				f, ok := tqf.(*callPlanExpression)
				if !ok {
					return nil, sql3.NewErrInternalf("unexpected time quantum filter expression type: %T", tqf)
				}
				// argument 0 should be a column ref
				arg, ok := f.args[0].(*qualifiedRefPlanExpression)
				if !ok {
					return nil, sql3.NewErrInternalf("unexpected time quantum filter argument expression type: %T", f.args[0])
				}
				if strings.EqualFold(arg.columnName, c) {
					expr, err := i.planner.generatePQLCallFromExpr(ctx, tqf)
					if err != nil {
						return nil, err
					}

					call.Children = append(call.Children, expr)
					foundInTimeQuantumFilters = true
				}
			}

			if !foundInTimeQuantumFilters {
				call.Children = append(call.Children,
					&pql.Call{
						Name: "Rows",
						Args: map[string]interface{}{"field": c},
					},
				)
			}
		}

		tbl, err := i.planner.schemaAPI.TableByName(ctx, core.TableName(i.tableName))
		if err != nil {
			return nil, sql3.NewErrTableNotFound(0, 0, i.tableName)
		}

		queryResponse, err := i.planner.executor.Execute(ctx, tbl, &pql.Query{Calls: []*pql.Call{call}}, nil, nil)
		if err != nil {
			return nil, err
		}

		extbl, ok := queryResponse.Results[0].(core.ExtractedTable)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected Extract() result type: %T", queryResponse.Results[0])
		}

		i.result = extbl.Columns

		//set the source index
		for idx, fld := range extbl.Fields {
			mappedColumn, ok := i.columnMap[fld.Name]
			if !ok {
				return nil, sql3.NewErrInternalf("mapped column not found for column named '%s'", fld.Name)
			}
			mappedColumn.srcColumnIdx = idx
		}
	}

	if len(i.result) > 0 {
		row := make([]interface{}, i.rowWidth)

		for _, c := range i.columns {
			result := i.result[0]

			mappedColumn, ok := i.columnMap[c]
			if !ok {
				return nil, sql3.NewErrInternalf("mapped column not found for column named '%s'", c)
			}
			mappedColIdx := mappedColumn.columnIdx
			mappedSrcColIdx := mappedColumn.srcColumnIdx

			if strings.EqualFold(c, string(core.PrimaryKeyFieldName)) {
				if result.Column.Keyed {
					row[mappedColIdx] = result.Column.Key
				} else {
					row[mappedColIdx] = int64(result.Column.ID)
				}
			} else {
				switch mappedColumn.dataType.(type) {
				case *parser.DataTypeIDSet:
					val, ok := result.Rows[mappedSrcColIdx].([]uint64)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type for column value '%T'", result.Rows[mappedSrcColIdx])
					}
					if val == nil {
						row[mappedColIdx] = nil
					} else {
						row[mappedColIdx] = val
					}

				case *parser.DataTypeStringSet:
					val, ok := result.Rows[mappedSrcColIdx].([]string)
					if !ok {
						return nil, sql3.NewErrInternalf("unexpected type for column value '%T'", result.Rows[mappedSrcColIdx])
					}
					if val == nil {
						row[mappedColIdx] = nil
					} else {
						row[mappedColIdx] = val
					}

				default:
					row[mappedColIdx] = result.Rows[mappedSrcColIdx]
				}
			}
		}

		// Move to next result element.
		i.result = i.result[1:]
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}
