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

	// Stores address of predecessor node
	predecessorAddr string

	// fingerTable contains the list of fingers
	// associated with a node.
	fingerTable []*Finger

	// store stores the key-value pairs assigned to
	// the node.
	store dataStore

	// channel to indicate node is exiting
	exitCh chan struct{}
}

// Each ith finger represents the node with is
// atleast at a distance of 2^(i - 1) from
// current node
type Finger struct {

	// Id of n + 2^(i - 1) node
	id []byte

	// address of n + 2^(i - 1) node
	address *string
}

// Successor method find the successor of given id.
// Successor node of id N is the first node whose id is
// either equal to N or follows N (in clockwise fashnion).
func (node *Node) Successor(id []byte, rpcAddr *string) error {

	// If the id is between node and its successor
	// then return the successor
	if betweenRightInc(id, node.id, node.fingerTable[0].id) {
		*rpcAddr = *node.fingerTable[0].address
		return nil
	}

	pred, address := node.closest_preceeding_node(id)
	var predId []byte
	pred.Call("Node.GetId", "", &predId)

	if equal(node.id, predId) {
		// If the closest preceeding node and
		// current node are same, return pred
		*rpcAddr = address
		return nil
	} else {
		// If they are different, call Successor function
		// on pred and return its result
		var newAddress string
		pred.Call("Node.Successor", id, &newAddress)
		*rpcAddr = newAddress
		return nil
	}
}

// Find the node closest to the given id with the help
// of current node's finger table
func (node *Node) closest_preceeding_node(id []byte) (*rpc.Client, string) {
	fingerIndex := len(node.fingerTable) - 1

	// Go through finger table from last entry
	// to first and return the first finger that
	// fulfills the criteria:
	// finger.id ɛ (node.id, id)
	for ; fingerIndex >= 0; fingerIndex-- {
		finger := node.fingerTable[fingerIndex]

		if between(finger.id, node.id, id) {
			client, err := getClient(finger.address)
			if err != nil {
				// If we are not able to get client of the closest
				// finger. Try remaining fingers.
				continue
			}
			return client, *finger.address
		}
	}

	// If no such finger is found return
	// the current node

	return node.self, node.address
}

// Check if 'n' is the predecessor of 'node'
func (node *Node) Notify(predAddr *string, _ *string) error {
	// if predecessor is nil or if n ɛ (current predecessor, node)
	// set it as predecessor
	predRPC, _ := getClient(predAddr)
	var predId []byte

	predRPC.Call("Node.GetId", "", &predId)

	if node.predecessorId == nil || between(predId, node.predecessorId, node.id) {
		node.makePredecessorNil()
		node.predecessorRPC = predRPC
		node.predecessorId = predId
		node.predecessorAddr = *predAddr
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
	myPred := node.predecessorRPC
	var reply string
	if myPred == nil {
		return ErrNilPredecessor
	}

	callReply := myPred.Go("Node.Check", "Hello", &reply, nil)

	select {
	case <-callReply.Done:
		if reply != "Acknowledged" {
			node.makePredecessorNil()
			return ErrFailedToReach
		}
	case <-time.NewTimer(5 * time.Second).C:
		node.makePredecessorNil()
		return ErrFailedToReach
	}
	return nil
}

func (node *Node) makePredecessorNil() {
	node.predecessorId = nil
	node.predecessorRPC = nil
	node.predecessorAddr = ""
}

// Fixes i th finger
func (node *Node) fixFinger(i int) int {
	// find successor of i th offset and
	// set it as i th finger of current node

	fingerId := fingerId(node.id, i)
	var successorAddr string
	node.Successor(fingerId, &successorAddr)

	successorRPC, _ := getClient(&successorAddr)

	var successorId []byte
	successorRPC.Call("Node.GetId", "", &successorId)

	if node.fingerTable[i] == nil {
		node.fingerTable[i] = new(Finger)
	}

	node.fingerTable[i].id = successorId
	node.fingerTable[i].address = &successorAddr

	return i + 1
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
func (node *Node) GetPredecessor(_ *string, reply *string) error {
	if node.predecessorId != nil {
		*reply = node.predecessorAddr
		return nil
	}
	return ErrNilPredecessor
}

// get current successor's predecessor node
// (this might not be same as the node calling this function
// i.e the current node)
// and check if it is better suited to be the successor
// of current node.
func (node *Node) stabilize() {
	successor := node.fingerTable[0]

	successorRPC, err := getClient(successor.address)
	if err != nil && err.Error() == ErrUnableToDial.Error() {
		return
	}

	var successorPredAddr string
	err = successorRPC.Call("Node.GetPredecessor", "", &successorPredAddr)

	if err != nil {
		// our successor does not know we are its predecessor
		// or we are our own successor
		if err.Error() == ErrNilPredecessor.Error() && !equal(node.id, successor.id) {
			successorRPC.Call("Node.Notify", node.address, "")
			return
		}
		// if error recieved was other than ErrNilPredecessor
		// or we are our own successor then do nothing.
		return
	}

	successorPredRPC, _ := getClient(&successorPredAddr)

	var predId []byte
	successorPredRPC.Call("Node.GetId", "", &predId)

	if between(predId, node.id, node.fingerTable[0].id) {
		node.fingerTable[0].id = predId
		node.fingerTable[0].address = &successorPredAddr
	}

	successorPredRPC.Call("Node.Notify", node.address, "")
}

func (node *Node) SetData(data *map[string]string, _ *string) error {
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

func (node *Node) TransferData(to *string, _ *string) error {
	toRPC, _ := getClient(to)

	var toId []byte
	toRPC.Call("Node.GetId", "", &toId)
	delKeys, transfer := node.store.getTransferRange(node.predecessorId, toId)

	toRPC.Call("Node.SetData", &transfer, "")
	node.deleteKeys(delKeys)

	return nil
}

// manually set successor of node
func (node *Node) SetSuccessor(successorAddr *string, _ *string) error {
	successorRPC, _ := getClient(successorAddr)

	var successorId []byte
	successorRPC.Call("Node.GetId", "", &successorId)
	node.fingerTable[0].id = successorId
	node.fingerTable[0].address = successorAddr

	return nil
}

// manually set predecessor of node
func (node *Node) SetPredecessor(predAddr *string, _ *string) error {
		node.makePredecessorNil()
	predRPC, _ := getClient(predAddr)

	var predId []byte
	predRPC.Call("Node.GetId", &predId, "")
	node.makePredecessorNil()
	node.predecessorId = predId
	node.predecessorRPC = predRPC
	node.predecessorAddr = *predAddr
	return nil
}

// This method is called when node is leaving the
// chord network. It does the following tasks
// 	1.transfers its keys to its successor
// 	2.connect its predecessor and successor to
// 	  each other
func (node *Node) Stop() {
	close(node.exitCh)

	successor := node.fingerTable[0]
	myPred := node.predecessorId
	if successor.id != nil && !equal(successor.id, node.id) {
		node.self.Call("Node.TransferData", successor.address, "")
		successorRPC, _ := getClient(successor.address)
		if myPred != nil {
			myPredRPC, _ := getClient(&node.predecessorAddr)
			myPredRPC.Call("Node.SetSuccessor", &successor.address, "")
			successorRPC.Call("Node.SetPredecessor", &node.predecessorAddr, "")
		}
	}

	node.self.Close()
	node.listener.Close()
}
