// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import (
	"context"

	"github.com/gernest/rbf/api"
	"github.com/gernest/rbf/ql/core"
)

// Ensure type implements interface.
var _ api.Schema = (*systemTableDefinitionsWrapper)(nil)

type systemTableDefinitionsWrapper struct {
	schemaAPI api.Schema
}

func newSystemTableDefinitionsWrapper(api api.Schema) *systemTableDefinitionsWrapper {
	return &systemTableDefinitionsWrapper{
		schemaAPI: api,
	}
}

func (s *systemTableDefinitionsWrapper) TableByName(ctx context.Context, tname core.TableName) (*core.Table, error) {
	return s.schemaAPI.TableByName(ctx, tname)

}

func (s *systemTableDefinitionsWrapper) TableByID(ctx context.Context, tid core.TableID) (*core.Table, error) {
	return s.schemaAPI.TableByID(ctx, tid)
}

func (s *systemTableDefinitionsWrapper) Tables(ctx context.Context) ([]*core.Table, error) {
	return s.schemaAPI.Tables(ctx)
}
