// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/gernest/rbf/ql/core"
	"github.com/gernest/rbf/ql/pql"
	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

// PlanOpPQLGroupBy plan operator handles a PQL group by with a single aggregate
type PlanOpPQLGroupBy struct {
	planner      *ExecutionPlanner
	tableName    string
	filter       types.PlanExpression
	aggregate    types.Aggregable
	groupByExprs []types.PlanExpression

	warnings []string
}

func NewPlanOpPQLGroupBy(p *ExecutionPlanner, tableName string, groupByExprs []types.PlanExpression, filter types.PlanExpression, aggregate types.Aggregable) *PlanOpPQLGroupBy {
	return &PlanOpPQLGroupBy{
		planner:      p,
		tableName:    tableName,
		groupByExprs: groupByExprs,
		filter:       filter,
		aggregate:    aggregate,
		warnings:     make([]string, 0),
	}
}

func (p *PlanOpPQLGroupBy) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
	result["tableName"] = p.tableName
	if p.filter != nil {
		result["filter"] = p.filter.Plan()
	}
	result["aggregate"] = p.aggregate.String()
	ps := make([]interface{}, 0)
	for _, e := range p.groupByExprs {
		ps = append(ps, e.Plan())
	}
	result["groupByColumns"] = ps
	return result
}

func (p *PlanOpPQLGroupBy) String() string {
	return ""
}

func (p *PlanOpPQLGroupBy) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpPQLGroupBy) Warnings() []string {
	return p.warnings
}

func (p *PlanOpPQLGroupBy) Schema() types.Schema {
	result := make(types.Schema, len(p.groupByExprs)+1)
	for idx, expr := range p.groupByExprs {
		ref, ok := expr.(*qualifiedRefPlanExpression)
		if !ok {
			continue
		}
		s := &types.PlannerColumn{
			ColumnName:   ref.columnName,
			RelationName: ref.tableName,
			Type:         expr.Type(),
		}
		result[idx] = s
	}
	s := &types.PlannerColumn{
		ColumnName:   p.aggregate.String(),
		RelationName: "",
		Type:         p.aggregate.Type(),
	}
	result[len(p.groupByExprs)] = s

	return result
}

func (p *PlanOpPQLGroupBy) Children() []types.PlanOperator {
	return []types.PlanOperator{}
}

func (p *PlanOpPQLGroupBy) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return &pqlGroupByRowIter{
		planner:        p.planner,
		tableName:      p.tableName,
		groupByColumns: p.groupByExprs,
		aggregate:      p.aggregate,
		filter:         p.filter,
	}, nil
}

func (p *PlanOpPQLGroupBy) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	return nil, nil
}

// pqlGroupByRowIter is an iterator for the PlanOpPQLGroupBy operator
// it provides rows consisting of the group by columns in the order they
// were specified and lastly the aggregate
type pqlGroupByRowIter struct {
	planner        *ExecutionPlanner
	tableName      string
	groupByColumns []types.PlanExpression
	filter         types.PlanExpression
	aggregate      types.Aggregable

	result []core.GroupCount
}

var _ types.RowIterator = (*pqlGroupByRowIter)(nil)

func (i *pqlGroupByRowIter) Next(ctx context.Context) (types.Row, error) {
	if i.result == nil {

		var cond *pql.Call
		var err error

		err = i.planner.checkAccess(ctx, i.tableName, accessTypeReadData)
		if err != nil {
			return nil, err
		}

		cond, err = i.planner.generatePQLCallFromExpr(ctx, i.filter)
		if err != nil {
			return nil, err
		}

		call := &pql.Call{
			Name: "GroupBy",
			Args: map[string]interface{}{},
		}
		for _, c := range i.groupByColumns {
			ref, ok := c.(types.IdentifiableByName)
			if !ok {
				return nil, sql3.NewErrInternalf("unexpected expression type in group by list '%T'", c)
			}
			//don't ask for the _id field
			if ref.Name() != string(core.PrimaryKeyFieldName) {
				call.Children = append(call.Children,
					&pql.Call{
						Name: "Rows",
						Args: map[string]interface{}{"_field": ref.Name()},
					},
				)
			}
		}

		// Apply filter & aggregate, if set.
		aggExpr, ok := i.aggregate.FirstChildExpr().(*qualifiedRefPlanExpression)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected aggregate expression type '%T'", i.aggregate.FirstChildExpr())
		}

		switch i.aggregate.(type) {
		case *countPlanExpression, *countStarPlanExpression:
			//nop

		case *countDistinctPlanExpression:
			aggregate := &pql.Call{
				Name: "Count",
				Children: []*pql.Call{{
					Name: "Distinct",
					Args: map[string]interface{}{"field": aggExpr.columnName},
				}},
			}
			call.Args["aggregate"] = aggregate

		case *sumPlanExpression, *avgPlanExpression:
			aggregate := &pql.Call{
				Name: "Sum",
				Args: map[string]interface{}{"field": aggExpr.columnName},
			}
			call.Args["aggregate"] = aggregate

		case *percentilePlanExpression:
			return nil, sql3.NewErrAggregateNotAllowedInGroupBy(0, 0, "PERCENTILE()")

		case *minPlanExpression:
			return nil, sql3.NewErrAggregateNotAllowedInGroupBy(0, 0, "MIN()")

		case *maxPlanExpression:
			return nil, sql3.NewErrAggregateNotAllowedInGroupBy(0, 0, "MAX()")

		default:
			return nil, sql3.NewErrInternalf("unexpected agg function type: '%T'", i.aggregate)
		}
		if cond != nil {
			call.Args["filter"] = cond
		}

		tbl, err := i.planner.schemaAPI.TableByName(ctx, core.TableName(i.tableName))
		if err != nil {
			return nil, sql3.NewErrTableNotFound(0, 0, i.tableName)
		}

		queryResponse, err := i.planner.executor.Execute(ctx, tbl, &pql.Query{Calls: []*pql.Call{call}}, nil, nil)
		if err != nil {
			return nil, err
		}

		gcs, ok := queryResponse.Results[0].(*core.GroupCounts)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected Extract() result type: %T", queryResponse.Results[0])
		}

		i.result = gcs.Groups()
	}

	if len(i.result) > 0 {
		//row width is group by columns + aggregate
		row := make([]interface{}, len(i.groupByColumns)+1)

		group := i.result[0]

		//populate all the group by columns
		for idx, c := range i.groupByColumns {

			g := group.Group[idx]
			if g.Value != nil {
				row[idx] = *g.Value
			} else if g.RowKey != "" {
				switch c.Type().(type) {
				case *parser.DataTypeStringSet:
					row[idx] = []string{g.RowKey}
				default:
					row[idx] = g.RowKey
				}
			} else {
				switch c.Type().(type) {
				case *parser.DataTypeIDSet:
					row[idx] = []uint64{g.RowID}
				default:
					row[idx] = int64(g.RowID)
				}
			}
		}
		//now populate the aggregate value
		aggIdx := len(i.groupByColumns)
		switch agg := i.aggregate.(type) {
		case *countPlanExpression, *countStarPlanExpression:
			row[aggIdx] = int64(group.Count)

		case *countDistinctPlanExpression:
			row[aggIdx] = int64(group.Agg)

		case *sumPlanExpression:
			switch ty := agg.Type().(type) {
			case *parser.DataTypeDecimal:
				if group.DecimalAgg == nil {
					row[aggIdx] = pql.NewDecimal(int64(group.Agg), ty.Scale)
				} else {
					row[aggIdx] = *group.DecimalAgg
				}
			case *parser.DataTypeInt:
				row[aggIdx] = int64(group.Agg)
			default:
				return nil, sql3.NewErrInternalf("unhandled sum return type '%T'", ty)
			}

		case *avgPlanExpression:
			if group.DecimalAgg == nil {
				average := float64(group.Agg) / float64(group.Count)
				row[aggIdx] = pql.NewDecimal(int64(average*10000), 4)
			} else {
				average := group.DecimalAgg.Float64() / float64(group.Count)
				row[aggIdx] = pql.NewDecimal(int64(average*10000), 4)
			}
		default:
			return nil, sql3.NewErrInternalf("unhandled aggregate function type '%T'", i.aggregate)
		}

		// Move to next result element.
		i.result = i.result[1:]
		return row, nil
	}
	return nil, types.ErrNoMoreRows
}
