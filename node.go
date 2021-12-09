package chord

import (
	"database/sql"
	"fmt"
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

	// predecessor is the first node in anti-clockwise
	// direction from current node. i.e. the node just
	// before current node in circular fashion.

	// rpc client of predecessor node
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

	// Store data in sqlite db to sync with frontend
	db *sql.DB
}

// Each ith finger represents the node which is
// atleast at a distance of 2^(i - 1) from
// current node
type Finger struct {

	// Id of n + 2^(i - 1) node
	id []byte

	// address of n + 2^(i - 1) node
	address string
}

// Find the finger just preceeding the given id from
// the node's finger table.
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
			return client, finger.address
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

// Check if current successor has failed
func (node *Node) checkSuccessor() *rpc.Client {
	try := 3

	// try to get successor rpc
	node.mutex.RLock()
	successor, err := getClient(node.fingerTable[0].address)
	node.mutex.RUnlock()

	// if we are unable to dail address of successor's rpc server
	// try re-dailing after 1 second break.
	if err != nil && err.Error() == ErrUnableToDial.Error() {
		for ; err.Error() == ErrUnableToDial.Error() && try > 0; try-- {
			time.Sleep(time.Second)

			node.mutex.RLock()
			successor, err = getClient(node.fingerTable[0].address)
			node.mutex.RUnlock()

			if err == nil {
				// error is nil i.e. we were able to dail the
				// rpc server's address. break out of for loop
				break
			}
		}
	}
	if try <= 0 {
		// if we were unable to get rpc client
		// in 3 tries make successor nil
		node.makeSuccessorNil()
		return node.self
	}
	return successor
}

// Make node's successor pointer point to
// itself indicating that it doesn't know
// its successor
func (node *Node) makeSuccessorNil() {
	node.mutex.Lock()
	defer node.mutex.Unlock()
	node.fingerTable[0].id = node.id
	node.fingerTable[0].address = node.address
	go updateSuccessor(node.db, node.address, node.fingerTable[0].address)

}

// Fixes the i'th finger
func (node *RPCNode) fixFinger(i int) int {
	// find successor of i th offset and
	// set it as i th finger of current node

	fingerId := fingerId(node.id, i)
	var successorAddr string
	node.Successor(fingerId, &successorAddr)

	successorRPC, err := getClient(successorAddr)

	if err != nil {
		// keep trying to dial rpc server for given
		// amount of tries.
		try := 3
		for ; err.Error() == ErrUnableToDial.Error() && try > 0; try-- {
			time.Sleep(time.Second)
			node.Successor(fingerId, &successorAddr)
			successorRPC, err = getClient(successorAddr)
			if err == nil {
				// successfully dailed rpc server.
				// break out of loop
				break
			}
		}
		if try <= 0 {
			return i
		}
	}

	// get id of successor of fingerId
	var successorId []byte
	if successorAddr != node.address {
		successorRPC.Call("RPCNode.GetId", "", &successorId)
	} else {
		successorId = node.id
	}

	successorRPC.Close()

	node.mutex.Lock()

	// Fix the i'th finger

	if node.fingerTable[i] == nil {
		node.fingerTable[i] = new(Finger)
	}

	node.fingerTable[i].id = successorId
	node.fingerTable[i].address = successorAddr

	if i == 0 {
		go updateSuccessor(node.db, node.address, successorAddr)
	}
	node.mutex.Unlock()

	// next finger to fix is i + 1
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
	// get rpc client of successor
	node.mutex.RLock()
	successor := node.fingerTable[0]
	node.mutex.RUnlock()

	successorRPC, err := getClient(successor.address)
	if err != nil && err.Error() == ErrUnableToDial.Error() {
		successorRPC = node.checkSuccessor()
	}

	// get predecessor of our successor
	var successorPredAddr string
	err = successorRPC.Call("RPCNode.GetPredecessor", "", &successorPredAddr)

	defer successorRPC.Close()
	if err != nil {
		// If our successor does't have a predecessor
		// and we are not our own successor.
		if err.Error() == ErrNilPredecessor.Error() && !equal(node.id, successor.id) {
			// Notify our successor that we might be its predecessor
			successorRPC.Call("RPCNode.Notify", node.address, "")
			return
		}
		// if error was not ErrNilPredecessor
		// or we are our own successor, then do nothing.
		return
	}

	successorPredRPC, _ := getClient(successorPredAddr)
	defer successorPredRPC.Close()

	var successorPredId []byte
	successorPredRPC.Call("RPCNode.GetId", "", &successorPredId)

	// check if our successor's predecessor is a viable replacement
	// for our successor. If it is replace our successor and notify
	// our new successor.
	if between(successorPredId, node.id, node.fingerTable[0].id) {
		node.mutex.Lock()
		node.fingerTable[0].id = successorPredId
		node.fingerTable[0].address = successorPredAddr

		go updateSuccessor(node.db, node.address, successorPredAddr)

		node.mutex.Unlock()
		successorPredRPC.Call("RPCNode.Notify", node.address, "")
	}
	successorPredRPC.Close()
}

// Wrapper to dataStore.del
func (node *Node) deleteKeys(keys []string) {
	node.store.del(keys)
}

// This method is called when node is leaving the
// chord network. It does the following tasks
// 	1.transfers its keys to its successor
// 	2.connect its predecessor and successor to
// 	  each other
func (node *Node) Stop() {
	fmt.Println("\nStoping -", toBigInt(node.id))
	var wg sync.WaitGroup
	wg.Add(1)
	go deleteNode(node.db, node.address, &wg)

	close(node.exitCh)

	node.mutex.RLock()
	successor := *(node.fingerTable)[0]
	node.mutex.RUnlock()

	// if the successor is known, transfer it the data
	if successor.id != nil && !equal(successor.id, node.id) {
		node.transferData(successor.address)
		successorRPC, _ := getClient(successor.address)
		// if predecessor if know, connect our successor
		// and predecessor to each other.
		if node.predecessorId != nil {
			node.predecessorRPC.Call("RPCNode.SetSuccessor", &successor.address, "")
			successorRPC.Call("RPCNode.SetPredecessor", &node.predecessorAddr, "")
			node.predecessorRPC.Close()
			successorRPC.Close()
		}
	}

	node.self.Close()
	node.listener.Close()
	wg.Wait()
}

// Saves key-value pair in chord network
func (node *Node) save(key, value string) string {
	fmt.Printf("Save %q : %q\n", key, value)

	var saveNodeAddr string

	// get hash of key
	keyHash := getHash(key)

	// find the node suitable to store the key and
	// get its rpc client
	node.self.Call("RPCNode.Successor", keyHash, &saveNodeAddr)
	saveNode, err := getClient(saveNodeAddr)

	if err != nil {
		for err.Error() == ErrUnableToDial.Error() {
			node.self.Call("RPCNode.Successor", keyHash, &saveNodeAddr)
			saveNode, err = getClient(saveNodeAddr)
		}
	}

	data := make(dataStore)
	data[key] = value

	// save the data on the node
	saveNode.Call("RPCNode.SetData", &data, "")
	saveNode.Close()
	return saveNodeAddr
}

// Transfer data to the node whose address is given by
// "to" parameter
func (node *Node) transferData(to string) {
	toRPC, err := getClient(to)

	if err != nil {
		fmt.Println("TransferData", err)
		return
	}

	defer toRPC.Close()

	var toId []byte

	node.mutex.RLock()
	// if transfering data to successor used the
	// saved id. For other nodes initiate rpc
	// to get the id for that node.
	if to == node.fingerTable[0].address {
		toId = node.fingerTable[0].id
	} else {
		toRPC.Call("RPCNode.GetId", "", &toId)
	}
	node.mutex.RUnlock()

	// get which data to transfer
	delKeys, transfer := node.getTransferRange(to, toId)

	// transfer the data
	toRPC.Call("RPCNode.SetData", &transfer, "")

	// delete data from this node
	node.deleteKeys(delKeys)
}

// Finds and returns which key-value pairs are eligible for transfer
func (node *Node) getTransferRange(to string, toID []byte) ([]string, dataStore) {
	delKeys := make([]string, 0)
	transfer := make(dataStore)

	fmt.Println("Transfering to", to)

	node.mutex.RLock()

	// check if node is stopping.
	// value of ok will be changed
	// to false if it is stopping.
	ok := true
	select {
	case _, ok = <-node.exitCh:
	default:
	}

	// transfer all data to successor only if successor
	// node and predecessor node are not same or if the
	// current node is stopping
	if !ok ||
		(equal(toID, node.fingerTable[0].id) &&
			!equal(node.fingerTable[0].id, node.predecessorId)) {
		transfer = node.store
		for key := range node.store {
			delKeys = append(delKeys, key)
		}
	} else {
		// else trasnfer only selected keys
		//
		// transfer keys from current node which do not lie
		// in the interval between toId and node.id (node.id inclusive)
		for key, value := range node.store {
			if !betweenRightInc(getHash(key), toID, node.id) {
				delKeys = append(delKeys, key)
				transfer[key] = value
			}
		}
	}
	node.mutex.RUnlock()

	fmt.Println(transfer)
	return delKeys, transfer
}
