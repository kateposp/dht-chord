package chord

import "fmt"

// This structure houses rpc methods of Node
type RPCNode struct {
	// promoted anonymous field
	*Node
}

// Successor method find the successor of given id.
// Successor node of id N is the first node whose id is
// either equal to N or follows N (in clockwise fashnion).
func (node *RPCNode) Successor(id []byte, rpcAddr *string) error {

	// If the id is between node and its successor
	// then return the successor
	node.mutex.RLock()
	if betweenRightInc(id, node.id, node.fingerTable[0].id) {
		*rpcAddr = node.fingerTable[0].address
		node.mutex.RUnlock()
		return nil
	}
	node.mutex.RUnlock()

	// find the closest preceeding node for given id
	pred, address := node.closest_preceeding_node(id)
	var predId []byte
	pred.Call("RPCNode.GetId", "", &predId)
	defer pred.Close()

	node.mutex.RLock()
	if equal(node.id, predId) {
		// If the closest preceeding node and
		// current node are same, return the
		// address of closest preceeding node
		*rpcAddr = address
		node.mutex.RUnlock()
		return nil
	} else {
		node.mutex.RUnlock()
		// If they are different, call Successor function
		// on closest preceeding node and return its result
		var newAddress string
		pred.Call("RPCNode.Successor", id, &newAddress)
		*rpcAddr = newAddress
		return nil
	}
}

// Check if node pointed by predAddr is the predecessor
func (node *RPCNode) Notify(predAddr *string, _ *string) error {
	// get rpc client
	predRPC, _ := getClient(*predAddr)

	var predId []byte
	predRPC.Call("RPCNode.GetId", "", &predId)

	if node.predecessorId == nil || between(predId, node.predecessorId, node.id) {
		// if our predecessor is nil or if node pointed by predId
		// is better suited to be our predecessor then replace
		// our predecessor

		// Transfer any data which might belong to our new
		// predecessor
		node.transferData(*predAddr)

		node.makePredecessorNil()

		node.mutex.Lock()
		// set new details
		node.predecessorRPC = predRPC
		node.predecessorId = predId
		node.predecessorAddr = *predAddr
		node.mutex.Unlock()
	}
	return nil
}

// Function to check if RPC server is responding
func (node *RPCNode) Check(arg *string, reply *string) error {
	*reply = "Acknowledged"
	return nil
}

// Return the id of the node
func (node *RPCNode) GetId(_ *string, id *[]byte) error {
	node.mutex.RLock()
	defer node.mutex.RUnlock()
	*id = node.id
	return nil
}

// Returns predecessor of the node
func (node *RPCNode) GetPredecessor(_ *string, reply *string) error {
	node.mutex.RLock()
	defer node.mutex.RUnlock()
	if node.predecessorId != nil {
		*reply = node.predecessorAddr
		return nil
	}
	return ErrNilPredecessor
}

// Saves data into node's store
func (node *RPCNode) SetData(data *map[string]string, _ *string) error {
	fmt.Println("Setting [")
	for key, value := range *data {
		fmt.Println(key, ":", value, ",")
		node.store.set(key, value)
	}
	fmt.Println("]")
	return nil
}

// Returns the value associated with following key
// if the node has the pair in its store else returns
// an error
func (node *RPCNode) GetValue(key *string, value *string) error {
	var ok bool
	*value, ok = node.store.get(*key)
	if !ok {
		return ErrNoKeyValuePair
	}
	return nil
}

// manually set successor of node
func (node *RPCNode) SetSuccessor(successorAddr *string, _ *string) error {
	// If successorAddr is same our address
	// then set ourself as our successor
	if *successorAddr == node.address {
		node.mutex.Lock()
		node.fingerTable[0].address = node.address
		node.fingerTable[0].id = node.id
		node.mutex.Unlock()
		return nil
	}

	// Update successor details in accordance to
	// the new successor

	successorRPC, _ := getClient(*successorAddr)
	defer successorRPC.Close()

	var successorId []byte
	successorRPC.Call("RPCNode.GetId", "", &successorId)

	node.mutex.Lock()
	node.fingerTable[0].id = successorId
	node.fingerTable[0].address = *successorAddr
	node.mutex.Unlock()

	return nil
}

// manually set predecessor of node
func (node *RPCNode) SetPredecessor(predAddr *string, _ *string) error {
	// If predAddr is same as our address
	// make our predecessor nil
	if *predAddr == node.address {
		node.makePredecessorNil()
		return nil
	}

	// Update predecessor details in accordance
	// to the new predecessor
	predRPC, _ := getClient(*predAddr)

	var predId []byte
	predRPC.Call("RPCNode.GetId", "", &predId)

	node.makePredecessorNil()

	node.mutex.Lock()
	node.predecessorId = predId
	node.predecessorRPC = predRPC
	node.predecessorAddr = *predAddr
	node.mutex.Unlock()
	return nil
}

// Retrieve a key-value pair from chord network
func (node *RPCNode) Retrieve(key *string, value *string) error {

	// Find where the key is stored
	var getNodeAddr string
	node.self.Call("RPCNode.Successor", getHash(*key), &getNodeAddr)

	getNode, _ := getClient(getNodeAddr)
	defer getNode.Close()

	// Get the value corresponding to the key
	// from the node which stores the key
	var val string
	err := getNode.Call("RPCNode.GetValue", &key, &val)

	// make val equal to error string if there
	// is an error in getting the value
	if err != nil {
		val = err.Error()
	}

	// set the value variable
	*value = val
	return nil
}
