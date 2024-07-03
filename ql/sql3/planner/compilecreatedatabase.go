// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

// compileCreateDatabaseStatement compiles a CREATE DATABASE statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileCreateDatabaseStatement(stmt *parser.CreateDatabaseStatement) (_ types.PlanOperator, err error) {
	return nil, sql3.NewErrUnsupported(stmt.Create.Line, stmt.Create.Column, true, "a CREATE DATABASE statement")
}

// analyzeCreateDatabaseStatement analyzes a CREATE DATABASE statement and
// returns an error if anything is invalid.
func (p *ExecutionPlanner) analyzeCreateDatabaseStatement(stmt *parser.CreateDatabaseStatement) error {
	return sql3.NewErrUnsupported(stmt.Create.Line, stmt.Create.Column, true, "a CREATE DATABASE statement")
}
