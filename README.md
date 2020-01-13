Base-2 merkelized trie, stored in files.
====================================================

Authenticated, versioned key value store. Aims to be faster then other existing stores implemented
on top of the leveldb/rocksdb (tendermint merkle iavl tree, ethereum patricia trie).
Implementation is based on the design proposed in [handshake whitepaper](https://handshake.org/files/handshake.txt), section `FFMT (Flat-File Merkle Tree)`.

#### API Overview

**Work in progress**

To open a store:

```golang
db, _ := store.Open(store.ProdDefaultConfig("path/to/dir"))
tree := urkeltrie.NewTree(db)
```

To write entries:

```golang
tree.Put([]byte("key"), []byte("value"))
```

To write a prehashed value:

```golang
tree.PutRaw([32]byte{1,2,3}, nil, []byte("value))
```

Key will be hashed internally to 32 byte digest, and used for a placement in a trie.
Key preimage, together with a value will be persisted on disk separately.

All dirty entries are kept in memory until tree is commited:

```golang
tree.Commit()
```

On commit tree and values are written to disk, all writes are append-only, followed by fsync.

Snapshot readers will not observe any dirty state, and can be used concurrently with commites to the tip of the tree.
You can use snapshot of the latest or any version that is still kept in store:

```golang
last := tree.Snapshot()
versioned, err := tree.VersionSnapshot(10)
```

To get a value or a value with a proof that it exists/doesn't exist:

```golang
proof := NewProof(0)
tree.GenerateProof([]byte("test"), proof)

proof.VerifyMembership(tree.Hash(), []byte("test"))
proof.VerifyNonMembership(tree.Hash(), []byte("test"))
```

Proof can be compactly marshalled to bytes, skipping empty nodes hashes.

#### Benchmarks

Last benchmarks for commision of 10,000 leaves in batches, until whole db size is 10,000,000 leaves.

Time spent for insertion:

![time](https://github.com/dshulyak/merklecmp/blob/master/_assets/time-u-10000.png)

More information about profiling, and how urkel compares to other authenticated stores can be found in [separate repository](https://github.com/dshulyak/merklecmp).