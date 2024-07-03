// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

func (p *ExecutionPlanner) compileShowDatabasesStatement(ctx context.Context, stmt parser.Statement) (types.PlanOperator, error) {
	return nil, sql3.NewErrUnsupported(0, 0, true, "a SHOW DATABASE statement")
}

func (p *ExecutionPlanner) compileShowTablesStatement(ctx context.Context, stmt *parser.ShowTablesStatement) (types.PlanOperator, error) {
	return nil, sql3.NewErrUnsupported(stmt.Show.Line, stmt.Show.Column, true, "a SHOW TABLES statement")
}

func (p *ExecutionPlanner) compileShowColumnsStatement(ctx context.Context, stmt *parser.ShowColumnsStatement) (_ types.PlanOperator, err error) {
	return nil, sql3.NewErrUnsupported(stmt.Show.Line, stmt.Show.Column, true, "a SHOW COLUMNS statement")
}

func (p *ExecutionPlanner) compileShowCreateTableStatement(ctx context.Context, stmt *parser.ShowCreateTableStatement) (_ types.PlanOperator, err error) {
	return nil, sql3.NewErrUnsupported(stmt.Show.Line, stmt.Show.Column, true, "a SHOW CREATE TABLE statement")
}
