WIP Flat file base-2 merkelized trie (or urkel trie)
====================================================

Based on the design proposed for handshake blockchain.


Lots of things are missing and probably not robust, some preliminary benchmarks for commisioning 5000 entries, full db size 5,000,000 entries without a cache and no buffered io, and in general very few optimizations.

```
goos: linux
goarch: amd64
pkg: github.com/dshulyak/urkeltrie
BenchmarkCommitPersistent5000Entries-8              1000         301464943 ns/op	221861716 B/op   1291535 allocs/op
PASS
ok      github.com/dshulyak/urkeltrie	307.079s
```
