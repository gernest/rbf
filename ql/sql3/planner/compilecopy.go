// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

// compileCopyStatement compiles a parser.CopyStatement AST into a PlanOperator
func (p *ExecutionPlanner) compileCopyStatement(stmt *parser.CopyStatement) (types.PlanOperator, error) {
	return nil, sql3.NewErrUnsupported(stmt.Copy.Line, stmt.Copy.Column, true, "COPY  statement")
}

func (p *ExecutionPlanner) analyzeCopyStatement(ctx context.Context, stmt *parser.CopyStatement) error {
	return sql3.NewErrUnsupported(stmt.Copy.Line, stmt.Copy.Column, true, "COPY  statement")
}
