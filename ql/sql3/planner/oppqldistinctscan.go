// Copyright 2023 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/gernest/rbf/ql/core"
	"github.com/gernest/rbf/ql/pql"
	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
	"github.com/gernest/rows"
)

// PlanOpPQLDistinctScan plan operator handles a PQL distinct scan
// i.e. a scan with only one column that is used in a distinct query
type PlanOpPQLDistinctScan struct {
	planner   *ExecutionPlanner
	tableName string
	column    string
	filter    types.PlanExpression
	topExpr   types.PlanExpression
	warnings  []string
}

func NewPlanOpPQLDistinctScan(p *ExecutionPlanner, tableName string, column string) (*PlanOpPQLDistinctScan, error) {
	return &PlanOpPQLDistinctScan{
		planner:   p,
		tableName: tableName,
		column:    column,
		warnings:  make([]string, 0),
	}, nil
}

func (p *PlanOpPQLDistinctScan) Plan() map[string]interface{} {
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
	result["column"] = p.column
	return result
}

func (p *PlanOpPQLDistinctScan) String() string {
	return ""
}

func (p *PlanOpPQLDistinctScan) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpPQLDistinctScan) Warnings() []string {
	return p.warnings
}

func (p *PlanOpPQLDistinctScan) Name() string {
	return p.tableName
}

func (p *PlanOpPQLDistinctScan) IsFilterable() bool {
	return true
}

func (p *PlanOpPQLDistinctScan) UpdateFilters(filterCondition types.PlanExpression) (types.PlanOperator, error) {
	p.filter = filterCondition
	return p, nil
}

func (p *PlanOpPQLDistinctScan) UpdateTimeQuantumFilters(filters ...types.PlanExpression) (types.PlanOperator, error) {
	return p, nil
}

func (p *PlanOpPQLDistinctScan) Schema() types.Schema {
	result := make(types.Schema, 0)

	tname := core.TableName(p.tableName)
	table, err := p.planner.schemaAPI.TableByName(context.Background(), tname)
	if err != nil {
		return result
	}

	for _, fld := range table.Fields {
		if strings.EqualFold(string(fld.Name), p.column) {
			result = append(result, &types.PlannerColumn{
				ColumnName:   string(fld.Name),
				RelationName: p.tableName,
				Type:         fieldSQLDataType(fld),
			})
			break
		}
	}
	return result
}

func (p *PlanOpPQLDistinctScan) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpPQLDistinctScan) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &distinctScanRowIter{
		planner:   p.planner,
		tableName: p.tableName,
		column:    p.column,
		predicate: p.filter,
		topExpr:   p.topExpr,
	}, nil
}

func (p *PlanOpPQLDistinctScan) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return nil, nil
}

type distinctScanRowIter struct {
	planner   *ExecutionPlanner
	tableName string
	column    string
	predicate types.PlanExpression
	topExpr   types.PlanExpression

	result         []interface{}
	rowWidth       int
	columnDataType parser.ExprDataType
}

var _ types.RowIterator = (*distinctScanRowIter)(nil)

func (i *distinctScanRowIter) Next(ctx context.Context) (types.Row, error) {
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
		i.rowWidth = 1

		for _, fld := range table.Fields {
			if strings.EqualFold(i.column, string(fld.Name)) {
				i.columnDataType = fieldSQLDataType(fld)
				break
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
		call := &pql.Call{
			Name:     "Distinct",
			Args:     map[string]interface{}{"field": i.column},
			Children: []*pql.Call{cond},
		}

		queryResponse, err := i.planner.executor.Execute(ctx, table, &pql.Query{Calls: []*pql.Call{call}}, nil, nil)
		if err != nil {
			return nil, err
		}
		switch res := queryResponse.Results[0].(type) {
		case *rows.Row:
			result := make([]interface{}, 0)
			if len(res.Keys) > 0 {
				for _, n := range res.Keys {
					result = append(result, n)
				}
			} else {
				for _, n := range res.Columns() {
					result = append(result, int64(n))
				}
			}
			i.result = result

		case core.SignedRow:
			result := make([]interface{}, 0)

			negs := res.Neg.Columns()
			pos := res.Pos.Columns()
			for _, n := range negs {
				result = append(result, -(int64(n)))
			}
			for _, n := range pos {
				result = append(result, int64(n))
			}
			i.result = result

		case core.DistinctTimestamp:
			result := make([]interface{}, 0)
			for _, n := range res.Values {
				if tm, err := time.ParseInLocation(time.RFC3339Nano, n, time.UTC); err == nil {
					result = append(result, tm)
				} else {
					return nil, sql3.NewErrInternalf("unable to convert to time.Time: %v", n)
				}
			}
			i.result = result

		default:
			return nil, sql3.NewErrInternalf("unexpected Distinct() result type: %T", res)
		}
	}

	if len(i.result) > 0 {
		row := make([]interface{}, i.rowWidth)

		result := i.result[0]

		switch t := i.columnDataType.(type) {

		case *parser.DataTypeBool:
			val, ok := result.(int64)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected type for column value '%T'", result)
			}
			row[0] = val == 1

		case *parser.DataTypeDecimal:
			val, ok := result.(int64)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected type for column value '%T'", result)
			}
			row[0] = pql.NewDecimal(val, t.Scale)

		case *parser.DataTypeIDSet:
			val, ok := result.(int64)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected type for column value '%T'", result)
			}
			row[0] = []uint64{uint64(val)}

		case *parser.DataTypeStringSet:
			val, ok := result.(string)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected type for column value '%T'", result)
			}
			row[0] = []string{val}

		default:
			row[0] = result
		}

		// Move to next result element.
		i.result = i.result[1:]
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}
