// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

// compileInsertStatement compiles an INSERT statement into a PlanOperator.
func (p *ExecutionPlanner) compileInsertStatement(ctx context.Context, stmt *parser.InsertStatement) (_ types.PlanOperator, err error) {
	return nil, sql3.NewErrUnsupported(stmt.Insert.Line, stmt.Insert.Column, true, "a INSERT statement")
}

// analyzeInsertStatement analyzes an INSERT statement and returns and error if
// anything is invalid.
func (p *ExecutionPlanner) analyzeInsertStatement(ctx context.Context, stmt *parser.InsertStatement) error {
	return sql3.NewErrUnsupported(stmt.Insert.Line, stmt.Insert.Column, true, "a INSERT statement")
}
