# dht-chord
Distributed Hash Table based on Chord protocol

> A distributed hash table (DHT) is a distributed system that provides a lookup service similar to a hash table: key-value pairs are stored in a DHT, and any participating node can efficiently retrieve the value associated with a given key. The main advantage of a DHT is that nodes can be added or removed with minimum work around re-distributing keys, this allows a DHT to scale to extremely large numbers of nodes and to handle continual node arrivals, departures, and failures.

\- [Distributed hash table](https://en.wikipedia.org/wiki/Distributed_hash_table)

> Chord is a protocol and algorithm for a peer-to-peer distributed hash table. Chord specifies how keys are assigned to nodes, and how a node can discover the value for a given key by first locating the node responsible for that key.

\- [Chord (peer-to-peer)](https://en.wikipedia.org/wiki/Chord_(peer-to-peer))

## Sources
* https://github.com/arriqaaq/chord
* [Chord: A Scalable Peer-to-peer Lookup Protocol for Internet Applications - 
Ion Stoica
, Robert Morris
, David Liben-Nowell
, David R. Karger
, M. Frans Kaashoek
, Frank Dabek
, Hari Balakrishnan](https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf)