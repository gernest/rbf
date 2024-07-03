// Copyright 2023 Molecula Corp. All rights reserved.

package planner

import (
	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

// compileCreateFunctionStatement compiles a parser.CreateFunctionStatement AST into a PlanOperator
func (p *ExecutionPlanner) compileCreateFunctionStatement(stmt *parser.CreateFunctionStatement) (types.PlanOperator, error) {
	return nil, sql3.NewErrUnsupported(stmt.Create.Line, stmt.Create.Column, true, "a CREATE FUNCTION statement")
}

func (p *ExecutionPlanner) analyzeCreateFunctionStatement(stmt *parser.CreateFunctionStatement) error {
	return sql3.NewErrUnsupported(stmt.Create.Line, stmt.Create.Column, true, "a CREATE FUNCTION statement")
}
