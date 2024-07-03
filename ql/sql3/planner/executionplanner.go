// Copyright 2021 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"log/slog"

	"github.com/gernest/rbf/ql/api"
	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

func isDatabaseNotFoundError(err error) bool {
	return sql3.Is(err, sql3.ErrDatabaseNameDoesNotExist)
}

func isTableNotFoundError(err error) bool {
	return sql3.Is(err, sql3.ErrTableNameDoesNotExist)
}

// ExecutionPlanner compiles SQL text into a query plan
type ExecutionPlanner struct {
	executor       api.Executor
	schemaAPI      api.Schema
	systemLayerAPI api.SystemLayerAPI
	logger         *slog.Logger
	sql            string
}

func NewExecutionPlanner(executor api.Executor, schemaAPI api.Schema, systemLayerAPI api.SystemLayerAPI, logger *slog.Logger, sql string) *ExecutionPlanner {
	return &ExecutionPlanner{
		executor:       executor,
		schemaAPI:      newSystemTableDefinitionsWrapper(schemaAPI),
		systemLayerAPI: systemLayerAPI,
		logger:         logger,
		sql:            sql,
	}
}

// CompilePlan takes an AST (parser.Statement) and compiles into a query plan returning the root
// PlanOperator
// The act of compiling includes an analysis step that does semantic analysis of the AST, this includes
// type checking, and sometimes AST rewriting. The compile phase uses the type-checked and rewritten AST
// to produce a query plan.
func (p *ExecutionPlanner) CompilePlan(ctx context.Context, stmt parser.Statement) (types.PlanOperator, error) {
	// call analyze first
	err := p.analyzePlan(ctx, stmt)
	if err != nil {
		return nil, err
	}

	var rootOperator types.PlanOperator
	switch stmt := stmt.(type) {
	case *parser.SelectStatement:
		rootOperator, err = p.compileSelectStatement(stmt, false)
	case *parser.ShowDatabasesStatement:
		rootOperator, err = p.compileShowDatabasesStatement(ctx, stmt)
	case *parser.CopyStatement:
		rootOperator, err = p.compileCopyStatement(stmt)
	case *parser.PredictStatement:
		rootOperator, err = p.compilePredictStatement(ctx, stmt)
	case *parser.ShowTablesStatement:
		rootOperator, err = p.compileShowTablesStatement(ctx, stmt)
	case *parser.ShowColumnsStatement:
		rootOperator, err = p.compileShowColumnsStatement(ctx, stmt)
	case *parser.ShowCreateTableStatement:
		rootOperator, err = p.compileShowCreateTableStatement(ctx, stmt)
	case *parser.CreateDatabaseStatement:
		rootOperator, err = p.compileCreateDatabaseStatement(stmt)
	case *parser.CreateTableStatement:
		rootOperator, err = p.compileCreateTableStatement(ctx, stmt)
	case *parser.CreateViewStatement:
		rootOperator, err = p.compileCreateViewStatement(stmt)
	case *parser.AlterDatabaseStatement:
		rootOperator, err = p.compileAlterDatabaseStatement(ctx, stmt)
	case *parser.AlterTableStatement:
		rootOperator, err = p.compileAlterTableStatement(ctx, stmt)
	case *parser.AlterViewStatement:
		rootOperator, err = p.compileAlterViewStatement(stmt)
	case *parser.DropDatabaseStatement:
		rootOperator, err = p.compileDropDatabaseStatement(ctx, stmt)
	case *parser.DropTableStatement:
		rootOperator, err = p.compileDropTableStatement(ctx, stmt)
	case *parser.DropViewStatement:
		rootOperator, err = p.compileDropViewStatement(ctx, stmt)
	case *parser.DropModelStatement:
		rootOperator, err = p.compileDropModelStatement(stmt)
	case *parser.InsertStatement:
		rootOperator, err = p.compileInsertStatement(ctx, stmt)
	case *parser.BulkInsertStatement:
		rootOperator, err = p.compileBulkInsertStatement(ctx, stmt)
	case *parser.DeleteStatement:
		rootOperator, err = p.compileDeleteStatement(stmt)
	case *parser.CreateModelStatement:
		rootOperator, err = p.compileCreateModelStatement(stmt)
	case *parser.CreateFunctionStatement:
		rootOperator, err = p.compileCreateFunctionStatement(stmt)

	default:
		return nil, sql3.NewErrInternalf("cannot plan statement: %T", stmt)
	}
	// optimize the plan
	if err == nil {
		rootOperator, err = p.optimizePlan(ctx, rootOperator)
	}
	return rootOperator, err
}

func (p *ExecutionPlanner) analyzePlan(ctx context.Context, stmt parser.Statement) error {
	switch stmt := stmt.(type) {
	case *parser.SelectStatement:
		_, err := p.analyzeSelectStatement(ctx, stmt)
		return err
	case *parser.ShowDatabasesStatement:
		return nil
	case *parser.CopyStatement:
		return p.analyzeCopyStatement(ctx, stmt)
	case *parser.PredictStatement:
		return p.analyzePredictStatement(ctx, stmt)
	case *parser.ShowTablesStatement:
		return nil
	case *parser.ShowColumnsStatement:
		return nil
	case *parser.ShowCreateTableStatement:
		return nil
	case *parser.CreateDatabaseStatement:
		return p.analyzeCreateDatabaseStatement(stmt)
	case *parser.CreateTableStatement:
		return p.analyzeCreateTableStatement(stmt)
	case *parser.CreateViewStatement:
		return p.analyzeCreateViewStatement(ctx, stmt)
	case *parser.AlterDatabaseStatement:
		return p.analyzeAlterDatabaseStatement(stmt)
	case *parser.AlterTableStatement:
		return p.analyzeAlterTableStatement(stmt)
	case *parser.AlterViewStatement:
		return p.analyzeAlterViewStatement(ctx, stmt)
	case *parser.DropDatabaseStatement:
		return nil
	case *parser.DropTableStatement:
		return nil
	case *parser.DropViewStatement:
		return nil
	case *parser.DropModelStatement:
		return nil
	case *parser.InsertStatement:
		return p.analyzeInsertStatement(ctx, stmt)
	case *parser.BulkInsertStatement:
		return p.analyzeBulkInsertStatement(ctx, stmt)
	case *parser.DeleteStatement:
		return p.analyzeDeleteStatement(ctx, stmt)
	case *parser.CreateModelStatement:
		return p.analyzeCreateModelStatement(ctx, stmt)
	case *parser.CreateFunctionStatement:
		return p.analyzeCreateFunctionStatement(stmt)

	default:
		return sql3.NewErrInternalf("cannot analyze statement: %T", stmt)
	}
}

type accessType byte

const (
	accessTypeReadData accessType = iota
	accessTypeWriteData
	accessTypeCreateObject
	accessTypeAlterObject
	accessTypeDropObject
)

func (p *ExecutionPlanner) checkAccess(ctx context.Context, objectName string, _ accessType) error {
	return nil
}

func (e *ExecutionPlanner) mapper(ctx context.Context, op types.PlanOperator) (types.Rows, error) {

	var result types.Rows
	iter, err := op.Iterator(ctx, nil)
	if err != nil {
		return nil, err
	}
	row, err := iter.Next(ctx)
	if err != nil {
		if err != types.ErrNoMoreRows {
			return types.Rows{}, nil
		}
		return nil, err
	}
	for {
		result = append(result, row)
		row, err = iter.Next(ctx)
		if err != nil {
			if err != types.ErrNoMoreRows {
				return nil, err
			}
			return result, nil
		}
	}
}
