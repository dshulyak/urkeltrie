package urkeltrie

import (
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func setupTreeSmall(tb testing.TB) (*Tree, func()) {
	tmp, err := ioutil.TempDir("", "testing-urkel")
	require.NoError(tb, err)
	store, err := newFileStore(tmp, 4000)
	require.NoError(tb, err)
	return NewTree(store), func() { require.NoError(tb, os.RemoveAll(tmp)) }
}

func setupFullTree(tb testing.TB, hint int) *Tree {
	store, err := NewFileStore("")
	require.NoError(tb, err)
	tree := NewTree(store)
	var (
		key   = make([]byte, 20)
		value = make([]byte, 10)
	)
	for i := 0; i < hint; i++ {
		rand.Read(key)
		rand.Read(value)
		require.NoError(tb, tree.Put(key, value))
	}
	return tree
}

func setupFullTreeP(tb testing.TB, hint int) (*Tree, func()) {
	tmp, err := ioutil.TempDir("", "testing-urkel")
	require.NoError(tb, err)
	store, err := NewFileStore(tmp)
	require.NoError(tb, err)

	tree := NewTree(store)
	var (
		key   = make([]byte, 20)
		value = make([]byte, 10)
	)
	for i := 0; i < hint; i++ {
		rand.Read(key)
		rand.Read(value)
		require.NoError(tb, tree.Put(key, value))
	}
	return tree, func() {
		require.NoError(tb, os.RemoveAll(tmp))
	}
}

func TestTreeGet(t *testing.T) {
	rand.Seed(time.Now().Unix())
	store, err := NewFileStore("")
	require.NoError(t, err)
	tree := NewTree(store)
	var (
		added  = [][]byte{}
		values = [][]byte{}
	)
	for i := 0; i < 1000; i++ {
		key := make([]byte, 10)
		value := make([]byte, 5)
		rand.Read(key)
		rand.Read(value)
		require.NoError(t, tree.Put(key, value))
		added = append(added, key)
		values = append(values, value)
	}
	for i, key := range added {
		rst, err := tree.Get(key)
		require.NoError(t, err)
		require.Equal(t, values[i], rst)
	}
}

func TestTreeCommitPersistent(t *testing.T) {
	tmp, err := ioutil.TempDir("", "test-commit")
	require.NoError(t, err)
	defer os.RemoveAll(tmp)

	store, err := NewFileStore(tmp)
	require.NoError(t, err)

	var (
		added  = [][]byte{}
		values = [][]byte{}
		tree   = NewTree(store)
	)

	for i := 0; i < 33; i++ {
		key := make([]byte, 10)
		value := make([]byte, 5)
		rand.Read(key)
		rand.Read(value)
		require.NoError(t, tree.Put(key, value))
		added = append(added, key)
		values = append(values, value)
	}
	require.NoError(t, tree.Commit())
	for i, key := range added {
		rst, err := tree.Get(key)
		require.NoError(t, err)
		require.Equal(t, values[i], rst)
	}
}

func TestTreeProvePersistent(t *testing.T) {
	tree, closer := setupFullTreeP(t, 100)
	defer closer()

	var (
		key = make([]byte, 10)
	)
	rand.Read(key)
	require.NoError(t, tree.Put(key, key))

	root := tree.Hash()
	require.NoError(t, tree.Commit())

	proof := NewProof(256)
	require.NoError(t, tree.GenerateProof(key, proof))
	require.True(t, proof.VerifyMembership(root, key, key))
}

func TestTreeMultiFiles(t *testing.T) {
	tmp, err := ioutil.TempDir("", "testing-urkel")
	require.NoError(t, err)
	store, err := newFileStore(tmp, 10000)
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.RemoveAll(tmp))
	}()

	tree := NewTree(store)

	var (
		added  = [][][]byte{}
		values = [][][]byte{}
	)

	for i := 0; i < 5; i++ {
		round := [][]byte{}
		vals := [][]byte{}
		for i := 0; i < 5; i++ {
			key := make([]byte, 10)
			value := make([]byte, 400)
			rand.Read(key)
			rand.Read(value)
			require.NoError(t, tree.Put(key, value))
			round = append(round, key)
			vals = append(vals, value)
		}
		added = append(added, round)
		values = append(values, vals)
		require.NoError(t, tree.Commit())
	}
	for i := range added {
		for j := range added[i] {
			got, err := tree.Get(added[i][j])
			require.NoError(t, err)
			require.Equal(t, values[i][j], got)
		}
	}
}

func TestTreeGetMultiCommit(t *testing.T) {
	tree, closer := setupFullTreeP(t, 100)
	defer closer()

	var (
		added  = [][]byte{}
		values = [][]byte{}
	)
	for i := 0; i < 5; i++ {
		for i, key := range added {
			got, err := tree.Get(key)
			require.NoError(t, err)
			require.Equal(t, values[i], got)
		}
		added = added[:0]
		values = values[:0]
		for i := 0; i < 5; i++ {
			key := make([]byte, 10)
			value := make([]byte, 10)
			rand.Read(key)
			rand.Read(value)
			require.NoError(t, tree.Put(key, value))
			added = append(added, key)
			values = append(values, value)
		}
		require.NoError(t, tree.Commit())
	}
}

func TestFlushTree(t *testing.T) {
	tree, closer := setupTreeSmall(t)
	defer closer()

	ft := NewFlushTreeFromTree(tree, 10)
	for i := 0; i < 10; i++ {
		for j := 0; j < 10; j++ {
			key := make([]byte, 10)
			rand.Read(key)
			require.NoError(t, ft.Put(key, key))
		}
	}
}

func BenchmarkTreeGet(b *testing.B) {
	tree := setupFullTree(b, 10000)
	b.ResetTimer()
	key := make([]byte, 10)
	for i := 0; i < b.N; i++ {
		rand.Read(key)
		_, _ = tree.Get(key)
	}
}

func BenchmarkPut(b *testing.B) {
	tree := setupFullTree(b, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < 10000; j++ {
			key := make([]byte, 10)
			rand.Read(key)
			tree.Put(key, key)
		}
	}
}

type testTree interface {
	Put([]byte, []byte) error
	Commit() error
}

func benchmarkCommitPersistent(b *testing.B, tree testTree, commit int) {
	b.ResetTimer()
	total := 0
	for i := 0; i < b.N; i++ {
		start := time.Now()
		for j := 0; j < commit; j++ {
			key := make([]byte, 10)
			rand.Read(key)
			require.NoError(b, tree.Put(key, key))
		}
		require.NoError(b, tree.Commit())
		total += commit
		// make a plot for time as func for commited enties
		log.Printf("commit took %v. total %d", time.Since(start), total)
	}
}

func BenchmarkCommitPersistent5000Entries(b *testing.B) {
	tree, closer := setupFullTreeP(b, 0)
	defer closer()
	benchmarkCommitPersistent(b, tree, 5000)
}

func BenchmarkCommitPersistent40000Entries(b *testing.B) {
	tree, closer := setupFullTreeP(b, 0)
	defer closer()
	benchmarkCommitPersistent(b, tree, 40000)
}

func BenchmarkPeriodicWrites500Commit40000(b *testing.B) {
	tree, closer := setupFullTreeP(b, 0)
	defer closer()
	benchmarkCommitPersistent(b, NewFlushTreeFromTree(tree, 500), 40000)
}

func BenchmarkPeriodicWrites500Commit5000(b *testing.B) {
	tree, closer := setupFullTreeP(b, 0)
	defer closer()
	benchmarkCommitPersistent(b, NewFlushTreeFromTree(tree, 500), 5000)
}
