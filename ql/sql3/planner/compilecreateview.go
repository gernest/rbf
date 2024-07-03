// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

// compileCreateViewStatement compiles a parser.CreateViewStatement AST into a PlanOperator
func (p *ExecutionPlanner) compileCreateViewStatement(stmt *parser.CreateViewStatement) (types.PlanOperator, error) {
	return nil, sql3.NewErrUnsupported(stmt.Create.Line, stmt.Create.Column, true, "a CREATE VIEW statement")
}

// compileAlterViewStatement compiles a parser.AlterViewStatement AST into a PlanOperator
func (p *ExecutionPlanner) compileAlterViewStatement(stmt *parser.AlterViewStatement) (types.PlanOperator, error) {
	return nil, sql3.NewErrUnsupported(stmt.Alter.Line, stmt.Alter.Column, true, "a ALTER VIEW statement")
}

func (p *ExecutionPlanner) analyzeCreateViewStatement(ctx context.Context, stmt *parser.CreateViewStatement) error {
	return sql3.NewErrUnsupported(stmt.Create.Line, stmt.Create.Column, true, "a CREATE VIEW statement")
}

func (p *ExecutionPlanner) analyzeAlterViewStatement(ctx context.Context, stmt *parser.AlterViewStatement) error {
	return sql3.NewErrUnsupported(stmt.Alter.Line, stmt.Alter.Column, true, "a ALTER VIEW statement")
}
