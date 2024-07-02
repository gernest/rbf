package boolean

import (
	"github.com/gernest/rbf/dsl/cursor"
	"github.com/gernest/rbf/dsl/query"
	"github.com/gernest/rbf/dsl/tx"
	"github.com/gernest/rows"
)

type Match struct {
	field string
	value bool
}

func Filter(field string, value bool) *Match {
	return &Match{field: field, value: value}
}

var _ query.Filter = (*Match)(nil)

func (m *Match) Apply(tx *tx.Tx, columns *rows.Row) (*rows.Row, error) {
	c, err := tx.Tx.Cursor(m.field)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	var r *rows.Row
	if m.value {
		r, err = cursor.Row(c, tx.Shard, trueRowID)
	} else {
		r, err = cursor.Row(c, tx.Shard, falseRowID)
	}
	if err != nil {
		return nil, err
	}
	if columns != nil {
		r = r.Intersect(columns)
	}
	return r, nil
}
