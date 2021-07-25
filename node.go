package chord

import (
	"log"
	"math/big"
	"net"
	"net/rpc"
	"sync"
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

	// RW Mutex lock can be held by arbitary
	// no. of readers or a single writer
	mutex sync.RWMutex
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

// Find the node closest to the given id with the help
// of current node's finger table
func (node *Node) closest_preceeding_node(id []byte) (*rpc.Client, string) {
	fingerIndex := len(node.fingerTable) - 1

	// Go through finger table from last entry
	// to first and return the first finger that
	// fulfills the criteria:
	// finger.id ɛ (node.id, id)
	for ; fingerIndex >= 0; fingerIndex-- {
		node.mutex.RLock()
		finger := node.fingerTable[fingerIndex]
		if finger == nil {
			node.mutex.RUnlock()
			continue
		}

		if between(finger.id, node.id, id) {
			client, err := getClient(finger.address)
			if err != nil {
				// If we are not able to get client of the closest
				// finger. Try remaining fingers.
				node.mutex.RUnlock()
				continue
			}
			defer node.mutex.RUnlock()
			return client, *finger.address
		}
		node.mutex.RUnlock()
	}

	// If no such finger is found return
	// the current node
	return node.self, node.address
}

// Check if predecessor has failed or not
func (node *Node) checkPredecessor() error {
	node.mutex.RLock()
	myPred := node.predecessorRPC
	node.mutex.RUnlock()

	var reply string
	if myPred == nil {
		return ErrNilPredecessor
	}

	callReply := myPred.Go("RPCNode.Check", "Hello", &reply, nil)

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

// Make the fields corresponding to
// Predecessor nil / default value
func (node *Node) makePredecessorNil() {
	node.mutex.Lock()
	defer node.mutex.Unlock()
	// Close old predecessor RPC client
	if node.predecessorRPC != nil {
		node.predecessorRPC.Close()
	}

	node.predecessorId = nil
	node.predecessorRPC = nil
	node.predecessorAddr = ""
}

// Fixes i th finger
func (node *RPCNode) fixFinger(i int) int {
	// find successor of i th offset and
	// set it as i th finger of current node

	fingerId := fingerId(node.id, i)
	var successorAddr string
	node.Successor(fingerId, &successorAddr)

	successorRPC, err := getClient(&successorAddr)

	if err != nil {
		// keep trying to dial rpc server for given
		// amount of tries.
		try := 3
		for ; err.Error() == ErrUnableToDial.Error() && try > 0; try-- {
			time.Sleep(time.Second)
			node.Successor(fingerId, &successorAddr)
			successorRPC, err = getClient(&successorAddr)
			if err == nil {
				break
			}
		}
		if try <= 0 {
			return i
		}
	}

	var successorId []byte
	if successorAddr != node.address {
		successorRPC.Call("RPCNode.GetId", "", &successorId)
	} else {
		successorId = node.id
	}

	successorRPC.Close()

	node.mutex.Lock()
	if node.fingerTable[i] == nil {
		node.fingerTable[i] = new(Finger)
	}

	node.fingerTable[i].id = successorId
	node.fingerTable[i].address = &successorAddr
	node.mutex.Unlock()

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

// get current successor's predecessor node
// (this might not be same as the node calling this function
// i.e the current node)
// and check if it is better suited to be the successor
// of current node.
func (node *Node) stabilize() {
	node.mutex.RLock()
	successor := node.fingerTable[0]
	node.mutex.RUnlock()

	successorRPC, err := getClient(successor.address)
	if err != nil && err.Error() == ErrUnableToDial.Error() {
		return
	}

	var successorPredAddr string
	err = successorRPC.Call("RPCNode.GetPredecessor", "", &successorPredAddr)

	defer successorRPC.Close()
	if err != nil {
		// our successor does not know we are its predecessor
		// or we are our own successor
		if err.Error() == ErrNilPredecessor.Error() && !equal(node.id, successor.id) {
			successorRPC.Call("RPCNode.Notify", node.address, "")
			return
		}
		// if error recieved was other than ErrNilPredecessor
		// or we are our own successor then do nothing.
		return
	}

	successorPredRPC, _ := getClient(&successorPredAddr)
	defer successorPredRPC.Close()

	var predId []byte
	successorPredRPC.Call("RPCNode.GetId", "", &predId)

	if between(predId, node.id, node.fingerTable[0].id) {
		node.mutex.Lock()
		node.fingerTable[0].id = predId
		node.fingerTable[0].address = &successorPredAddr
		node.mutex.Unlock()
	}

	successorPredRPC.Call("RPCNode.Notify", node.address, "")
}

// Wrapper to dataStore.del
func (node *Node) deleteKeys(keys []string) {
	node.store.del(keys)
}

// Transfers key-value pairs to a node
func (node *Node) TransferData(to *string, _ *string) error {
	toRPC, err := getClient(to)

	if err != nil {
		log.Println("TransferData", err)
		return nil
	}

	defer toRPC.Close()

	var toId []byte
	toRPC.Call("RPCNode.GetId", "", &toId)
	var delKeys []string
	var transfer dataStore

	// If ID of node and successor is equal
	// then predecessor must be nil
	// hence check keys in current node which
	// are eligible for transfer
	if equal(node.id, node.fingerTable[0].id) {
		delKeys, transfer = node.store.getTransferRange(node.id, toId)
	} else {
		delKeys, transfer = node.store.getTransferRange(node.predecessorId, toId)
	}
	toRPC.Call("RPCNode.SetData", &transfer, "")
	node.deleteKeys(delKeys)

	return nil
}

// This method is called when node is leaving the
// chord network. It does the following tasks
// 	1.transfers its keys to its successor
// 	2.connect its predecessor and successor to
// 	  each other
func (node *Node) Stop() {
	log.Println("Stoping -", toBigInt(node.id))
	close(node.exitCh)

	node.mutex.RLock()
	successor := node.fingerTable[0]
	node.mutex.RUnlock()

	if successor.id != nil && !equal(successor.id, node.id) {
		node.self.Call("RPCNode.TransferData", successor.address, "")
		successorRPC, _ := getClient(successor.address)
		if node.predecessorId != nil {
			node.predecessorRPC.Call("RPCNode.SetSuccessor", &successor.address, "")
			successorRPC.Call("RPCNode.SetPredecessor", &node.predecessorAddr, "")
			node.predecessorRPC.Close()
			successorRPC.Close()
		}
	}

	node.self.Close()
	node.listener.Close()
}

// Saves key-value pair in chord network
func (node *Node) Save(key, value string) {
	log.Printf("Saving %q : %q", key, value)
	var saveNodeAddr string
	keyHash := getHash(key)
	node.self.Call("RPCNode.Successor", keyHash, &saveNodeAddr)
	saveNode, err := getClient(&saveNodeAddr)

	if err != nil {
		for err.Error() == ErrUnableToDial.Error() {
			node.self.Call("RPCNode.Successor", keyHash, &saveNodeAddr)
			saveNode, err = getClient(&saveNodeAddr)
		}
	}

	data := make(dataStore)
	data[key] = value
	saveNode.Call("RPCNode.SetData", &data, "")
	saveNode.Close()
}

func (node *Node) retrieve(key string) string {
	var getNodeAddr string
	node.self.Call("RPCNode.Successor", getHash(key), &getNodeAddr)

	getNode, _ := getClient(&getNodeAddr)
	defer getNode.Close()

	var value string
	getNode.Call("RPCNode.GetValue", &key, &value)
	return value
}
