package query

import (
	"io"

	"github.com/gernest/rows"
)

type RowIterator interface {
	io.Seeker
	Next() (*rows.Row, uint64, *int64, bool, error)
}
