// Copyright 2023 Molecula Corp. All rights reserved.

package planner

import (
	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

// compileDropModelStatement compiles a DROP MODEL statement into a PlanOperator.
func (p *ExecutionPlanner) compileDropModelStatement(stmt *parser.DropModelStatement) (_ types.PlanOperator, err error) {
	return nil, sql3.NewErrUnsupported(stmt.Drop.Line, stmt.Drop.Column, true, "a DROP MODEL statement")
}
