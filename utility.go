package chord

import (
	"bytes"
	"crypto/sha1"
	"math/big"
	"net/rpc"
)

// Check if value is between left and right bound
// (right bound inclusive)
func betweenRightInc(value, leftBound, rightBound []byte) bool {
	return equal(value, rightBound) || between(value, leftBound, rightBound)
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

// Check if two byte slices are equal
func equal(valOne, valTwo []byte) bool {
	return bytes.Equal(valOne, valTwo)
}

// create []byte of string by hashing it
// using sha1
func getHash(str string) []byte {
	h := sha1.New()
	h.Write([]byte(str))
	return h.Sum(nil)
}

func getClient(address string) (*rpc.Client, error) {
	client, err := rpc.DialHTTP("tcp", address)
	if err != nil {
		return &rpc.Client{}, ErrUnableToDial
	}
	return client, nil
}

func toBigInt(arr []byte) *big.Int {
	return (&big.Int{}).SetBytes(arr)
}
