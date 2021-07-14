package chord

import "errors"

var (
	ErrUnableToListen    = errors.New("error: rpc server unable to listen on specified addr:port")
	ErrUnableToDial      = errors.New("error: unable to dial the address of rpc server")
	ErrFailedToReach     = errors.New("error: unable to reach rpc server")
	ErrNodeAlreadyExists = errors.New("error: node with same id already exists")
)
