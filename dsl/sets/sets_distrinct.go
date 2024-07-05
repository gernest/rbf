package sets

import (
	"github.com/RoaringBitmap/roaring/roaring64"
	"github.com/gernest/rbf/dsl/mutex"
	"github.com/gernest/rbf/dsl/tx"
	"github.com/gernest/rows"
)

func Distinct(txn *tx.Tx, field string, o *roaring64.Bitmap, filters *rows.Row) error {
	return mutex.Distinct(txn, field, o, filters)
}
