// Copyright 2022 Molecula Corp. All rights reserved.

package planner

import "github.com/gernest/rbf/ql/sql3"

type functionSystemObject struct {
	name     string
	language string
	body     string
}

func (p *ExecutionPlanner) getFunctionByName(name string) (*functionSystemObject, error) {
	return nil, sql3.NewErrTableNotFound(0, 0, "fb_functions")
}
