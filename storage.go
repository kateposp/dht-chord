package chord

// dataStore is an alias to map data structure
// with string type keys and string type values
type dataStore map[string][]byte

type KeyValue struct {
	Key   string
	Value []byte
}

// Save a Key-Value pair
func (data dataStore) set(key string, value []byte) {
	data[key] = value
}

// Return the Value associated with the given Key
func (data dataStore) get(key string) ([]byte, bool) {
	value, ok := data[key]
	return value, ok
}

// Delete Key-Value pairs
func (data dataStore) del(keys []string) {
	for _, key := range keys {
		delete(data, key)
	}
}
