package chord

import (
	"bytes"
)

// Check if value is between left and right bound
// (right bound inclusive)
func betweenRightInc(value, leftBound, rightBound []byte) bool {
	return bytes.Equal(value, rightBound) || between(value, leftBound, rightBound)
}

// Check if value is between left and right bound
// (both bounds exclusive)
func between(value, leftBound, rightBound []byte) bool {
	switch bytes.Compare(leftBound, rightBound) {
	case -1:
		return bytes.Compare(leftBound, value) == -1 && bytes.Compare(value, rightBound) == -1

	case 1:
		return bytes.Compare(leftBound, value) == -1 || bytes.Compare(value, rightBound) == -1

	case 0:
		return true

	default:
		return false
	}
}
