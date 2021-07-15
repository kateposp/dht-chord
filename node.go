package chord

import (
	"math/big"
	"net"
	"net/rpc"
	"time"
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

	// listener for rpc server
	listener net.Listener

	// predecessor is the rpc client type of the first
	// node in anti-clockwise direction from current
	// node. i.e. the node just before current node
	// in circular fashion.
	predecessorRPC *rpc.Client

	// Stores id of predecessor node
	predecessorId []byte

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

// Successor method find the successor of given id.
// Successor node of id N is the first node whose id is
// either equal to N or follows N (in clockwise fashnion).
func (node *Node) Successor(id []byte, rpcClient *rpc.Client) error {
	// If the id is between node and its successor
	// then return the successor
	if betweenRightInc(id, node.id, node.fingerTable[0].id) {
		*rpcClient = *node.fingerTable[0].node
		return nil
	}

	pred := node.closest_preceeding_node(id)
	var predId []byte
	pred.Call("Node.GetId", "", predId)

	if equal(node.id, predId) {
		// If the closest preceeding node and
		// current node are same, return pred
		*rpcClient = pred // or *rpcClient = node.self
		return nil
	} else {
		// If they are different, call Successor funtion
		// on pred and return its result
		var newRpc rpc.Client
		pred.Call("Node.Successor", id, &newRpc)
		*rpcClient = newRpc
		return nil
	}
}

// Find the node closest to the given id with the help
// of current node's finger table
func (node *Node) closest_preceeding_node(id []byte) rpc.Client {
	fingerIndex := len(node.fingerTable) - 1

	// Go through finger table from last entry
	// to first and return the first finger that
	// fulfills the criteria:
	// finger.id ɛ (node.id, id)
	for ; fingerIndex >= 0; fingerIndex-- {
		finger := node.fingerTable[fingerIndex]

		if between(finger.id, node.id, id) {
			return *finger.node
		}
	}

	// If no such finger is found return
	// the current node
	return *node.self
}

// Check if 'n' is the predecessor of 'node'
func (node *Node) Notify(n *Node, _ *string) error {
	// if predecessor is nil or if n ɛ (current predecessor, node)
	// set it as predecessor
	if node.predecessorId == nil || between(n.id, node.predecessorId, node.id) {
		node.predecessorRPC = n.self
		node.predecessorId = n.id
	}
	return nil
}

// Function to check if RPC server is responding
func (node *Node) Check(arg *string, reply *string) error {
	*reply = "Acknowledged"
	return nil
}

// Check if predecessor has failed or not
func (node *Node) checkPredecessor() error {
	var reply string
	callReply := node.predecessorRPC.Go("Node.Check", "Hello", &reply, nil)

	select {
	case <-callReply.Done:
		if reply != "Acknowledged" {
			return ErrFailedToReach
		}
	case <-time.NewTimer(5 * time.Second).C:
		return ErrFailedToReach
	}
	return nil
}

// Fixes i th finger
func (node *Node) fixFinger(i int) {
	// find successor of i th offset and
	// set it as i th finger of current node

	fingerId := fingerId(node.id, i)
	var successor rpc.Client
	node.Successor(fingerId, &successor)

	var successorId []byte
	successor.Call("Node.GetId", "", &successorId)

	node.fingerTable[i].id = successorId
	node.fingerTable[i].node = &successor
}

// Returns the id (type []byte) of i th finger of node
//
// i th finger is at an offset of 2^(i - 1) from node
// in circular fashion.
//
// hence equation = {n + 2^(i -1)} mod (2^m)
// where m is the number of bits in hash
func fingerId(n []byte, i int) []byte {
	// Number of bits in sha1 hash
	m := 160

	// Convert the ID to a bigint
	idInt := (&big.Int{}).SetBytes(n)

	// Get the offset
	two := big.NewInt(2)
	offset := big.Int{}
	offset.Exp(two, big.NewInt(int64(i)), nil)

	// Sum
	sum := big.Int{}
	sum.Add(idInt, &offset)

	// Get the ceiling
	ceil := big.Int{}
	ceil.Exp(two, big.NewInt(int64(m)), nil)

	// Apply the mod
	idInt.Mod(&sum, &ceil)

	// Add together
	return idInt.Bytes()
}

// Return the id of the node
func (node *Node) GetId(_ *string, id *[]byte) error {
	*id = node.id
	return nil
}

// Returns predecessor of a node
func (node *Node) GetPredecessor(_ *string, reply *rpc.Client) error {
	*reply = *node.predecessorRPC
	return nil
}

// get current successor's predecessor node
// (this might not be same as the node calling this function
// i.e the current node)
// and check if it is better suited to be the successor
// of current node.
func (node *Node) stabilize() {
	var x rpc.Client
	node.fingerTable[0].node.Call("Node.GetPredecessor", "", &x)

	var xId []byte
	x.Call("Node.GetId", "", &xId)

	if between(xId, node.id, node.fingerTable[0].id) {
		node.fingerTable[0].node = &x
		node.fingerTable[0].id = xId
	}

	x.Call("Node.Notify", &node, "")
}

func (node *Node) SetData(data *dataStore, _ *string) error {
	for key, value := range *data {
		node.store.set(key, value)
	}
	return nil
}

func (node *Node) GetValue(key *string, value *string) error {
	var ok bool
	*value, ok = node.store.get(*key)
	if !ok {
		return ErrNoKeyValuePair
	}
	return nil
}

func (node *Node) deleteKeys(keys []string) {
	node.store.del(keys)
}

func (node *Node) TransferData(to *rpc.Client, _ *string) error {
	var toId []byte
	to.Call("Node.GetId", "", &toId)
	delKeys, transfer := node.store.getTransferRange(node.predecessorId, toId)

	to.Call("Node.SetData", &transfer, "")
	node.deleteKeys(delKeys)

	return nil
}

// manually set successor of node
func (node *Node) SetSuccessor(successor *rpc.Client, _ *string) error {
	var successorId []byte
	successor.Call("Node.GetId", "", &successorId)
	node.fingerTable[0].id = successorId
	node.fingerTable[0].node = successor

	return nil
}

func (node *Node) SetPredecessor(pred *rpc.Client, _ *string) error {
	var predId []byte
	pred.Call("Node.GetId", &predId, "")
	node.predecessorId = predId
	node.predecessorRPC = pred
	return nil
}

// This method is called when node is leaving the
// chord network. It does the following tasks
// 	1.transfers its keys to its successor
// 	2.connect its predecessor and successor to
// 	  each other
func (node *Node) stop() {
	successor := *node.fingerTable[0].node
	node.self.Call("Node.TransferData", &successor, "")
	node.predecessorRPC.Go("Node.SetSuccessor", &successor, "", nil)

	myPred := *node.predecessorRPC
	node.fingerTable[0].node.Go("Node.SetPredecessor", &myPred, "", nil)

	node.self.Close()
	node.listener.Close()
}
