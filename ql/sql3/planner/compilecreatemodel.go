// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"
	"strings"

	"github.com/gernest/rbf/ql/sql3"
	"github.com/gernest/rbf/ql/sql3/parser"
	"github.com/gernest/rbf/ql/sql3/planner/types"
)

// TODO (pok) what does 'if not exists' do?

// compileCreateModelStatement compiles a parser.CreateModelStatement AST into a PlanOperator
func (p *ExecutionPlanner) compileCreateModelStatement(stmt *parser.CreateModelStatement) (types.PlanOperator, error) {
	return nil, sql3.NewErrUnsupported(stmt.Create.Line, stmt.Create.Column, true, "a CREATE MODEL statement")
}

func (p *ExecutionPlanner) analyzeCreateModelStatement(ctx context.Context, stmt *parser.CreateModelStatement) error {
	return sql3.NewErrUnsupported(stmt.Create.Line, stmt.Create.Column, true, "a CREATE MODEL statement")
}

func (p *ExecutionPlanner) analyzeModelOptionExpr(ctx context.Context, optName string, expr parser.Expr, scope parser.Statement) (parser.Expr, error) {
	if expr == nil {
		return nil, nil
	}

	e, err := p.analyzeExpression(ctx, expr, scope)
	if err != nil {
		return nil, err
	}

	switch strings.ToLower(optName) {
	case "modeltype":

		// model type needs to be a string literal
		if !(e.IsLiteral() && typeIsString(e.DataType())) {
			return nil, sql3.NewErrStringLiteral(e.Pos().Line, e.Pos().Column)
		}
		ty, ok := e.(*parser.StringLit)
		if !ok {
			return nil, sql3.NewErrInternalf("unexpected type '%T'", e)
		}

		// these are the model types supported
		switch strings.ToLower(ty.Value) {
		case "linear_regresssion":
			break
		default:
			return nil, sql3.NewErrInternalf("unexpected model tyoe '%s'", ty.Value)
		}
		return e, nil

	case "labels":
		// labels needs to be a string array literal
		// TODO (pok) revist 'set' literals (should be array literal; type checking could be robustified etc.)
		if !e.IsLiteral() {
			return nil, sql3.NewErrInternalf("string array literal expected")
		}
		ok, baseType := typeIsSet(e.DataType())
		if !ok {
			return nil, sql3.NewErrInternalf("array expression expected")
		}
		if !typeIsString(baseType) {
			return nil, sql3.NewErrInternalf("string array expected")
		}
		return e, nil

	default:
		return nil, sql3.NewErrInternalf("unexpected option name '%s'", optName)
	}
}
