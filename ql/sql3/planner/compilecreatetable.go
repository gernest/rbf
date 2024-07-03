// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

// compileCreateTableStatement compiles a CREATE TABLE statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileCreateTableStatement(ctx context.Context, stmt *parser.CreateTableStatement) (_ types.PlanOperator, err error) {
	return nil, sql3.NewErrUnsupported(stmt.Create.Line, stmt.Create.Column, true, "a CREATE TABLE  statement")
}

// analyzeCreateTableStatement analyzes a CREATE TABLE statement and returns an
// error if anything is invalid.
func (p *ExecutionPlanner) analyzeCreateTableStatement(stmt *parser.CreateTableStatement) error {
	return sql3.NewErrUnsupported(stmt.Create.Line, stmt.Create.Column, true, "a CREATE TABLE  statement")
}
