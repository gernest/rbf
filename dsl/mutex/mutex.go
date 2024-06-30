package mutex

import (
	"github.com/gernest/roaring"
	"github.com/gernest/roaring/shardwidth"
)

func Add(m *roaring.Bitmap, id uint64, value uint64) {
	m.Add(value*shardwidth.ShardWidth + (id % shardwidth.ShardWidth))
}
