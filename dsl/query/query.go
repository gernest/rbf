package query

import (
	"github.com/gernest/rbf/dsl/tx"
	"github.com/gernest/rows"
)

// Filter  selects rows to read in a shard/view context.
type Filter interface {
	Apply(tx *tx.Tx, columns *rows.Row) (*rows.Row, error)
}
