// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"fmt"

	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

// PlanOpSubquery is an operator for a subquery
type PlanOpSubquery struct {
	ChildOp  types.PlanOperator
	warnings []string
}

func NewPlanOpSubquery(child types.PlanOperator) *PlanOpSubquery {
	return &PlanOpSubquery{
		ChildOp:  child,
		warnings: make([]string, 0),
	}
}

func (p *PlanOpSubquery) Schema() types.Schema {
	return p.ChildOp.Schema()
}

func (p *PlanOpSubquery) Iterator(ctx context.Context, row types.Row) (types.RowIterator, error) {
	return p.ChildOp.Iterator(ctx, row)
}

func (p *PlanOpSubquery) Children() []types.PlanOperator {
	return []types.PlanOperator{
		p.ChildOp,
	}
}

func (p *PlanOpSubquery) WithChildren(children ...types.PlanOperator) (types.PlanOperator, error) {
	if len(children) != 1 {
		return nil, sql3.NewErrInternalf("unexpected number of children '%d'", len(children))
	}
	return NewPlanOpSubquery(children[0]), nil
}

func (p *PlanOpSubquery) Plan() map[string]interface{} {
	result := make(map[string]interface{})
	result["_op"] = fmt.Sprintf("%T", p)
	result["_schema"] = p.Schema().Plan()
	result["child"] = p.ChildOp.Plan()
	return result
}

func (p *PlanOpSubquery) String() string {
	return ""
}

func (p *PlanOpSubquery) AddWarning(warning string) {
	p.warnings = append(p.warnings, warning)
}

func (p *PlanOpSubquery) Warnings() []string {
	var w []string
	w = append(w, p.warnings...)
	if p.ChildOp != nil {
		w = append(w, p.ChildOp.Warnings()...)
	}
	return w
}

func (p *PlanOpSubquery) Name() string {
	return ""
}
