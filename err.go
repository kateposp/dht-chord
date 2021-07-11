package chord

import "errors"

var (
	LISTEN_ERROR  = errors.New("error: rpc server unable to listen on specified addr:port")
	DIALING_ERROR = errors.New("error: unable to dial the address of rpc server")
)
