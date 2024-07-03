// Copyright 2023 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

// compileDropViewStatement compiles a DROP VIEW statement into a PlanOperator.
func (p *ExecutionPlanner) compileDropViewStatement(ctx context.Context, stmt *parser.DropViewStatement) (_ types.PlanOperator, err error) {
	return nil, sql3.NewErrUnsupported(stmt.Drop.Line, stmt.Drop.Column, true, "a DROP VIEW statement")
}
