package bsi

import (
	"math/bits"

	"github.com/gernest/rbf"
	"github.com/gernest/rbf/dsl/cursor"
	"github.com/gernest/rows"
)

// Operation identifier
type Operation int

const (
	// LT less than
	LT Operation = 1 + iota
	// LE less than or equal
	LE
	// EQ equal
	EQ
	NEQ
	// GE greater than or equal
	GE
	// GT greater than
	GT
	// RANGE range
	RANGE
)

// Compare compares value.
// Values should be in the range of the BSI (max, min).  If the value is outside the range, the result
// might erroneous.  The operation parameter indicates the type of comparison to be made.
// For all operations with the exception of RANGE, the value to be compared is specified by valueOrStart.
// For the RANGE parameter the comparison criteria is >= valueOrStart and <= end.
//
// Returns column ID's satisfying the operation.
func Compare(
	c *rbf.Cursor,
	shard uint64, op Operation,
	valueOrStart uint64, end uint64,
	columns *rows.Row) (*rows.Row, error) {
	var r *rows.Row
	var err error
	switch op {
	case LT:
		r, err = rangeLT(c, shard, valueOrStart, false)
	case LE:
		r, err = rangeLT(c, shard, valueOrStart, true)
	case GT:
		r, err = rangeGT(c, shard, valueOrStart, false)
	case GE:
		r, err = rangeGT(c, shard, valueOrStart, true)
	case EQ:
		r, err = rangeEQ(c, shard, valueOrStart)
	case NEQ:
		r, err = rangeNEQ(c, shard, valueOrStart)
	case RANGE:
		r, err = rangeBetween(c, shard, valueOrStart, end)
	default:
		return rows.NewRow(), nil
	}
	if err != nil {
		return nil, err
	}
	if columns != nil {
		r = r.Intersect(columns)
	}
	return r, nil
}

func rangeLT(c *rbf.Cursor, shard uint64, predicate uint64, allowEquality bool) (*rows.Row, error) {
	if predicate == 1 && !allowEquality {
		predicate, allowEquality = 0, true
	}

	// Start with set of columns with values set.
	b, err := cursor.Row(c, shard, bsiExistsBit)
	if err != nil {
		return nil, err
	}
	switch {
	case predicate == 0 && !allowEquality:
		// Match all negative integers.
		return rows.NewRow(), nil
	case predicate == 0 && allowEquality:
		// Match all integers that are either negative or 0.
		return rangeEQ(c, shard, 0)
	default:
		return rangeLTUnsigned(c, shard, b, 64, predicate, allowEquality)
	}
}

func rangeLTUnsigned(c *rbf.Cursor, shard uint64, filter *rows.Row, bitDepth, predicate uint64, allowEquality bool) (*rows.Row, error) {
	switch {
	case uint64(bits.Len64(predicate)) > bitDepth:
		fallthrough
	case predicate == (1<<bitDepth)-1 && allowEquality:
		// This query matches all possible values.
		return filter, nil
	case predicate == (1<<bitDepth)-1 && !allowEquality:
		// This query matches everything that is not (1<<bitDepth)-1.
		matches := rows.NewRow()
		for i := uint64(0); i < bitDepth; i++ {
			row, err := cursor.Row(c, shard, uint64(bsiOffsetBit+i))
			if err != nil {
				return nil, err
			}
			matches = matches.Union(filter.Difference(row))
		}
		return matches, nil
	case allowEquality:
		predicate++
	}

	// Compare intermediate bits.
	matched := rows.NewRow()
	remaining := filter
	for i := int(bitDepth - 1); i >= 0 && predicate > 0 && remaining.Any(); i-- {
		row, err := cursor.Row(c, shard, uint64(bsiOffsetBit+i))
		if err != nil {
			return nil, err
		}
		zeroes := remaining.Difference(row)
		switch (predicate >> uint(i)) & 1 {
		case 1:
			// Match everything with a zero bit here.
			matched = matched.Union(zeroes)
			predicate &^= 1 << uint(i)
		case 0:
			// Discard everything with a one bit here.
			remaining = zeroes
		}
	}

	return matched, nil
}

func rangeGT(c *rbf.Cursor, shard uint64, predicate uint64, allowEquality bool) (*rows.Row, error) {
	b, err := cursor.Row(c, shard, bsiExistsBit)
	if err != nil {
		return nil, err
	}
	switch {
	case predicate == 0 && !allowEquality:
		// Match all positive numbers except zero.
		nonzero, err := rangeNEQ(c, shard, 0)
		if err != nil {
			return nil, err
		}
		b = nonzero
		fallthrough
	case predicate == 0 && allowEquality:
		// Match all positive numbers.
		return b, nil
	default:
		// Match all positive numbers greater than the predicate.
		return rangeGTUnsigned(c, shard, b, 64, uint64(predicate), allowEquality)
	}
}

func rangeGTUnsigned(c *rbf.Cursor, shard uint64, filter *rows.Row, bitDepth, predicate uint64, allowEquality bool) (*rows.Row, error) {
prep:
	switch {
	case predicate == 0 && allowEquality:
		// This query matches all possible values.
		return filter, nil
	case predicate == 0 && !allowEquality:
		// This query matches everything that is not 0.
		matches := rows.NewRow()
		for i := uint64(0); i < bitDepth; i++ {
			row, err := cursor.Row(c, shard, uint64(bsiOffsetBit+i))
			if err != nil {
				return nil, err
			}
			matches = matches.Union(filter.Intersect(row))
		}
		return matches, nil
	case !allowEquality && uint64(bits.Len64(predicate)) > bitDepth:
		// The predicate is bigger than the BSI width, so nothing can be bigger.
		return rows.NewRow(), nil
	case allowEquality:
		predicate--
		allowEquality = false
		goto prep
	}

	// Compare intermediate bits.
	matched := rows.NewRow()
	remaining := filter
	predicate |= (^uint64(0)) << bitDepth
	for i := int(bitDepth - 1); i >= 0 && predicate < ^uint64(0) && remaining.Any(); i-- {
		row, err := cursor.Row(c, shard, uint64(bsiOffsetBit+i))
		if err != nil {
			return nil, err
		}
		ones := remaining.Intersect(row)
		switch (predicate >> uint(i)) & 1 {
		case 1:
			// Discard everything with a zero bit here.
			remaining = ones
		case 0:
			// Match everything with a one bit here.
			matched = matched.Union(ones)
			predicate |= 1 << uint(i)
		}
	}

	return matched, nil
}

func rangeBetween(c *rbf.Cursor, shard uint64, predicateMin, predicateMax uint64) (*rows.Row, error) {
	b, err := cursor.Row(c, shard, bsiExistsBit)
	if err != nil {
		return nil, err
	}

	switch {
	case predicateMin == predicateMax:
		return rangeEQ(c, shard, predicateMin)
	default:
		return rangeBetweenUnsigned(c, shard, b, predicateMin, predicateMax)
	}
}

func rangeBetweenUnsigned(c *rbf.Cursor, shard uint64, filter *rows.Row, predicateMin, predicateMax uint64) (*rows.Row, error) {
	switch {
	case predicateMax > (1<<64)-1:
		// The upper bound cannot be violated.
		return rangeGTUnsigned(c, shard, filter, 64, predicateMin, true)
	case predicateMin == 0:
		// The lower bound cannot be violated.
		return rangeLTUnsigned(c, shard, filter, 64, predicateMax, true)
	}

	// Compare any upper bits which are equal.
	diffLen := bits.Len64(predicateMax ^ predicateMin)
	remaining := filter
	for i := int(64 - 1); i >= diffLen; i-- {
		row, err := cursor.Row(c, shard, uint64(bsiOffsetBit+i))
		if err != nil {
			return nil, err
		}
		switch (predicateMin >> uint(i)) & 1 {
		case 1:
			remaining = remaining.Intersect(row)
		case 0:
			remaining = remaining.Difference(row)
		}
	}

	// Clear the bits we just compared.
	equalMask := (^uint64(0)) << diffLen
	predicateMin &^= equalMask
	predicateMax &^= equalMask

	var err error
	remaining, err = rangeGTUnsigned(c, shard, remaining, uint64(diffLen), predicateMin, true)
	if err != nil {
		return nil, err
	}
	remaining, err = rangeLTUnsigned(c, shard, remaining, uint64(diffLen), predicateMax, true)
	if err != nil {
		return nil, err
	}
	return remaining, nil
}

func rangeEQ(c *rbf.Cursor, shard uint64, predicate uint64, filter ...*rows.Row) (*rows.Row, error) {
	// Start with set of columns with values set.
	b, err := cursor.Row(c, shard, bsiExistsBit)
	if err != nil {
		return nil, err
	}
	if len(filter) > 0 {
		b = b.Intersect(filter[0])
	}
	bitDepth := bits.LeadingZeros64(predicate)
	// Filter any bits that don't match the current bit value.
	for i := int(bitDepth - 1); i >= 0; i-- {
		row, err := cursor.Row(c, shard, uint64(bsiOffsetBit+i))
		if err != nil {
			return nil, err
		}
		bit := (predicate >> uint(i)) & 1
		if bit == 1 {
			b = b.Intersect(row)
		} else {
			b = b.Difference(row)
		}
	}
	return b, nil
}

func rangeNEQ(c *rbf.Cursor, shard uint64, predicate uint64) (*rows.Row, error) {
	// Start with set of columns with values set.
	b, err := cursor.Row(c, shard, bsiExistsBit)
	if err != nil {
		return nil, err
	}

	// Get the equal bitmap.
	eq, err := rangeEQ(c, shard, predicate)
	if err != nil {
		return nil, err
	}

	// Not-null minus the equal bitmap.
	b = b.Difference(eq)

	return b, nil
}
