package chord

import (
	"net/rpc"
)

// Node is an individual entity/worker/machine
// in the chord network.
type Node struct {
	// SHA-1 hash of ip address and port of a machine
	// makes its id.
	// i.e. for example SHA-1-HASH('10.0.0.1:9988')
	id []byte

	// address is the ip address of Node with port
	// i.e. for example 10.0.0.1:9988
	address string

	// rpc client of this node
	self *rpc.Client

	// predecessor is the rpc client type of the first
	// node in anti-clockwise direction from current
	// node. i.e. the node just before current node
	// in circular fashion.
	predecessor *rpc.Client

	// fingerTable contains the list of fingers
	// associated with a node.
	fingerTable []*Finger

	// store stores the key-value pairs assigned to
	// the node.
	store dataStore
}

// Each ith finger represents the node with is
// atleast at a distance of 2^(i - 1) from
// current node
type Finger struct {

	// Id of n + 2^(i - 1) node
	id []byte

	// rpc client of n + 2^(i - 1) node
	node *rpc.Client
}

// dataStore is an alias to map data structure
// with string type keys and string type values
type dataStore map[string]string

// Successor method find the successor of given id.
// Successor node of id N is the first node whose id is
// either equal to N or follows N (in clockwise fashnion).
func (node *Node) Successor(id []byte, rpcClient *rpc.Client) error {
	if betweenRightInc(id, node.id, node.fingerTable[0].id) {
		*rpcClient = *node.fingerTable[0].node
		return nil
	}

	*rpcClient = node.closest_preceeding_node(id)
	return nil
}

// Find the node closest to the given id with the help
// of current node's finger table
func (node *Node) closest_preceeding_node(id []byte) rpc.Client {
	fingerIndex := len(node.fingerTable) - 1

	for ; fingerIndex >= 0; fingerIndex-- {
		finger := node.fingerTable[fingerIndex]

		if between(id, node.id, finger.id) {
			return *finger.node
		}
	}

	return *node.self
}
