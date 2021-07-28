package chord

import "fmt"

// dataStore is an alias to map data structure
// with string type keys and string type values
type dataStore map[string]string

// Save a key-value pair
func (data dataStore) set(key, value string) {
	data[key] = value
}

// Return the value associated with the given key
func (data dataStore) get(key string) (string, bool) {
	value, ok := data[key]
	return value, ok
}

// Delete key-value pairs
func (data dataStore) del(keys []string) {
	for _, key := range keys {
		delete(data, key)
	}
}

func (data dataStore) getTransferRange(to string, left, right []byte) ([]string, dataStore) {
	delKeys := make([]string, 1)
	transfer := make(dataStore)
	fmt.Println("Transfering to", to)

	for key, value := range data {
		if betweenRightInc(getHash(key), left, right) {
			delKeys = append(delKeys, key)
			transfer[key] = value
		}
	}
	fmt.Println(transfer)

	return delKeys, transfer
}
