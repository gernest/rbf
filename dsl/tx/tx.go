package tx

import (
	"fmt"

	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/tr"
)

type Tx struct {
	Tx    *rbf.Tx
	Shard uint64
	View  string
	Tr    *tr.Read
}

func (tx *Tx) Key(field string) string {
	return fmt.Sprintf("~%s;%s<", field, tx.View)
}

func (tx *Tx) Cursor(field string, f func(c *rbf.Cursor, tx *Tx) error) error {
	c, err := tx.Tx.Cursor(tx.Key(field))
	if err != nil {
		return err
	}
	defer c.Close()
	return f(c, tx)
}
