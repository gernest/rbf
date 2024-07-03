// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

// compileAlterDatabaseStatement compiles an ALTER DATABASE statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileAlterDatabaseStatement(ctx context.Context, stmt *parser.AlterDatabaseStatement) (_ types.PlanOperator, err error) {
	return nil, sql3.NewErrUnsupported(stmt.Alter.Line, stmt.Alter.Column, true, "ALTER DATABASE statement")
}

// analyzeAlterDatabaseStatement analyze an ALTER DATABASE statement and returns an
// error if anything is invalid.
func (p *ExecutionPlanner) analyzeAlterDatabaseStatement(stmt *parser.AlterDatabaseStatement) error {
	return sql3.NewErrUnsupported(stmt.Alter.Line, stmt.Alter.Column, true, "ALTER DATABASE statement")
}
