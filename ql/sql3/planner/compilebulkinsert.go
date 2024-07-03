// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

// compileBulkInsertStatement compiles a BULK INSERT statement into a
// PlanOperator.
func (p *ExecutionPlanner) compileBulkInsertStatement(ctx context.Context, stmt *parser.BulkInsertStatement) (_ types.PlanOperator, err error) {
	return nil, sql3.NewErrUnsupported(stmt.Bulk.Line, stmt.Bulk.Column, true, "BULK INSERT  statement")
}

// analyzeBulkInsertStatement analyzes a BULK INSERT statement and returns an
// error if anything is invalid.
func (p *ExecutionPlanner) analyzeBulkInsertStatement(ctx context.Context, stmt *parser.BulkInsertStatement) error {
	return sql3.NewErrUnsupported(stmt.Bulk.Line, stmt.Bulk.Column, true, "BULK INSERT  statement")
}
