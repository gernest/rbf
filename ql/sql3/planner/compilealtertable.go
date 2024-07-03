// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

type alterOperation int64

const (
	alterOpAdd alterOperation = iota
	alterOpDrop
	alterOpRename
	alterOpSet
)

// compileAlterTableStatement compiles an ALTER TABLE statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileAlterTableStatement(ctx context.Context, stmt *parser.AlterTableStatement) (_ types.PlanOperator, err error) {
	return nil, sql3.NewErrUnsupported(stmt.Alter.Line, stmt.Alter.Column, true, "ALTER TABLE statement")
}

// analyzeAlterTableStatement analyze an ALTER TABLE statement and returns an
// error if anything is invalid.
func (p *ExecutionPlanner) analyzeAlterTableStatement(stmt *parser.AlterTableStatement) error {
	return sql3.NewErrUnsupported(stmt.Alter.Line, stmt.Alter.Column, true, "ALTER TABLE statement")
}
