// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

// compilePredictStatement compiles a parser.PredictStatement AST into a PlanOperator
func (p *ExecutionPlanner) compilePredictStatement(ctx context.Context, stmt *parser.PredictStatement) (types.PlanOperator, error) {
	return nil, sql3.NewErrUnsupported(stmt.Predict.Line, stmt.Predict.Column, true, "a PREDICT statement")
}

func (p *ExecutionPlanner) analyzePredictStatement(ctx context.Context, stmt *parser.PredictStatement) error {
	return sql3.NewErrUnsupported(stmt.Predict.Line, stmt.Predict.Column, true, "a PREDICT statement")
}
