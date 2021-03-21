# Τoy-Chord
## About Chord [1]
Peer-to-peer systems and applications are distributed systems without any centralized control or hierarchical organization, where the software running at each
node is equivalent in functionality. Chord is a distributed lookup protocol that provides support for just one operation: given a key, it maps the
key onto a node. Data location can be easily implemented on top of Chord by associating a key with each data item, and storing the key/data item pair at the node to which the key maps. Chord adapts efficiently as nodes join and leave the system, and can answer queries even if the system is continuously changing.

## Description
In this work, we designed ToyChord, a simplified version of Chord [1]. The application was developed in **Python** using the **Flask** framework with **HTTP requests**, for support calls within the machine network. Ιt was tested with 10 different nodes, which "run" on 5 virtual machines provided to us by the *Okeanos service*. The basic functionalities that we implemented are:
* The division of the IDs space (nodes and objects)
* Routing
* Node arrival
* Node departure (graceful)
* Data replication

## Consistent Hashing
The consistent hash function assigns each node and key an 160-bit identifier using the hash function **SHA-1**. A node’s identifier is chosen by hashing the node’s IP address (with port), while a key identifier is produced by hashing the key.

## Node Application
Each node implements the insert (key, value), query (key) and delete (key) functions for <key, value> pairs where both the key and the value are stings. The system handles node arrivals (join (nodeID)) and node departures (depart (nodeID)). In this case, the affected nodes update the pointers to the previous and next node that is necessary to route messages and redistribute their keys, so each node is responsible for the correct keys.

## Client Application
We implemented a client (cli) which allows the user to perform the following:
```
insert < key, value > : A (key, value) pair is inserted or updated.
delete < key > : A (key, value) pair is deleted.
query < key > : The key is searched and the corresponding value is returned.
query * : All (key, value) pairs stored in DHT per node are returned.
join : A new node joins the Chord.
depart : A node gracefully departs.
overlay : Prints the network topology.
help : Explanation of the commands.
```
## Replication - Consistency
The system stores replicas of the data associated with a key, at the *k* nodes succeeding the primary node. The variable *k* is called **replication factor**. We implemented 2 types of consistency for replicas.

### Linearizability - Chain replication
A write always starts from the primary node that is responsible for a key and proceeds to the *k-1* successors they have replicas. The last node in the chain returns the write result. A read instead, returns the value from the last node in the chain.

### Eventual consistency
The changes are spread lazily in the replicas. So, a write request goes to the primary node that is responsible for the specific key and this node returns the result of write. It then sends the new value to the next *k-1* nodes. A read is returned from any node has a copy of the key it requests (at the risk of returning a stale value).

## Εxperiments - Results
The ultimate goal of the development of the above application was to perform a series of experiments, that will lead to a better understanding of concepts of distributed systems, such as replication with linearizability and eventual consistency. Specifically, we performed experiments for these two types of replication in case the replication factor (*k*) is 1, 3 and 5. That is, a total of 6 experiments to study the read and write throughput of the system. The results along with some diagrams are presented in the [report](https://github.com/chrisbetze/toy-chord/blob/ddb0a1cd14969f14a63a46af702b445e87bfaf5e/report.pdf).

## References
[1] Stoica, Ion, et al. "Chord: A scalable peer-to-peer lookup service for internet applications." ACM
SIGCOMM Computer Communication Review 31.4 (2001): 149-160.
