// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

// compileDropTableStatement compiles a DROP TABLE statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileDropTableStatement(ctx context.Context, stmt *parser.DropTableStatement) (_ types.PlanOperator, err error) {
	return nil, sql3.NewErrUnsupported(stmt.Drop.Line, stmt.Drop.Column, true, "a DROP TABLE statement")
}
