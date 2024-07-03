package extendiblehash

import (
	"github.com/cespare/xxhash/v2"
)

type Hashable interface {
	Hash() uint64
}

type Key []byte

// BEWARE - not concurrent!!
func (k Key) Hash() uint64 {
	return xxhash.Sum64(k)
}
