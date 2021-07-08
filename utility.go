package chord

import (
	"bytes"
)

// Check if value is between left and right bound
// (right bound inclusive)
func betweenRightInc(value, leftBound, rightBound []byte) bool {
	if bytes.Equal(value, rightBound) ||
		between(value, leftBound, rightBound) {
		return true
	}
	return false
}

// Check if value is between left and right bound
// (both bounds exclusive)
func between(value, leftBound, rightBound []byte) bool {
	if bytes.Compare(leftBound, value) == -1 &&
		bytes.Compare(value, rightBound) == -1 {
		return true
	}
	return false
}
