package tx

import (
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/tr"
)

type Tx struct {
	Tx    *rbf.Tx
	Shard uint64
	Tr    *tr.Read
}

func (tx *Tx) Cursor(field string, f func(c *rbf.Cursor, tx *Tx) error) error {
	c, err := tx.Tx.Cursor(field)
	if err != nil {
		return err
	}
	defer c.Close()
	return f(c, tx)
}
