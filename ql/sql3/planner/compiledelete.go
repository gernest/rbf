// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

// compileDeleteStatement compiles a parser.DeleteStatment AST into a PlanOperator
func (p *ExecutionPlanner) compileDeleteStatement(stmt *parser.DeleteStatement) (types.PlanOperator, error) {
	return nil, sql3.NewErrUnsupported(stmt.Delete.Line, stmt.Delete.Column, true, "a DELETE statement")
}

func (p *ExecutionPlanner) analyzeDeleteStatement(ctx context.Context, stmt *parser.DeleteStatement) error {
	return sql3.NewErrUnsupported(stmt.Delete.Line, stmt.Delete.Column, true, "a DELETE statement")
}
