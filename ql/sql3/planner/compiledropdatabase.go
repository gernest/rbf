// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

// compileDropDatabaseStatement compiles a DROP DATABASE statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileDropDatabaseStatement(ctx context.Context, stmt *parser.DropDatabaseStatement) (_ types.PlanOperator, err error) {
	return nil, sql3.NewErrUnsupported(stmt.Drop.Line, stmt.Drop.Column, true, "a DROP DATABASE statement")
}
