package tx

import (
	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/tr"
)

type Tx struct {
	Tx    *rbf.Tx
	Shard uint64
	View  string
	Tr    *tr.Read
}
