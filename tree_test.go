package urkeltrie

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/dshulyak/urkeltrie/store"
	"github.com/dshulyak/urkeltrie/utils"
	"github.com/stretchr/testify/require"
)

func setupProdTree(tb testing.TB) (*Tree, func()) {
	tmp, err := ioutil.TempDir("", "testing-prod-urkel")
	require.NoError(tb, err)
	store, err := store.NewFileStore(store.DefaultProdConfig(tmp))
	require.NoError(tb, err)
	return NewTree(store), func() { require.NoError(tb, os.RemoveAll(tmp)) }
}

func setupTreeSmall(tb testing.TB) (*Tree, func()) {
	tmp, err := ioutil.TempDir("", "testing-urkel")
	require.NoError(tb, err)
	conf := store.DevConfig(tmp)
	conf.MaxFileSize = 4096
	store, err := store.NewFileStore(conf)
	require.NoError(tb, err)
	return NewTree(store), func() { require.NoError(tb, os.RemoveAll(tmp)) }
}

func setupFullTree(tb testing.TB, hint int) *Tree {
	store, err := store.NewFileStore(store.DevConfig(""))
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
	require.NoError(tb, tree.Commit())
	return tree
}

func setupFullTreeP(tb testing.TB, hint int) (*Tree, func()) {
	tmp, err := ioutil.TempDir("", "testing-urkel")
	require.NoError(tb, err)
	store, err := store.NewFileStore(store.DevConfig(tmp))
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
	require.NoError(tb, tree.Commit())
	return tree, func() {
		require.NoError(tb, os.RemoveAll(tmp))
	}
}

func TestTreeGet(t *testing.T) {
	rand.Seed(time.Now().Unix())
	store, err := store.NewFileStore(store.DevConfig(""))
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
	tree, closer := setupFullTreeP(t, 0)
	defer closer()

	var (
		added  = [][]byte{}
		values = [][]byte{}
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

func TestTreeMultiFiles(t *testing.T) {
	tmp, err := ioutil.TempDir("", "testing-urkel")
	require.NoError(t, err)
	conf := store.DevConfig(tmp)
	conf.MaxFileSize = 10000
	store, err := store.NewFileStore(conf)
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
	for i := 0; i < 100; i++ {
		key := make([]byte, 10)
		rand.Read(key)
		require.NoError(t, ft.Put(key, key))
	}
}

func TestLoadLatest(t *testing.T) {
	tree, closer := setupFullTreeP(t, 0)
	defer closer()

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
	tree = NewTree(tree.store)
	require.NoError(t, tree.LoadLatest())

	for i := range added {
		for j := range added[i] {
			val, err := tree.Get(added[i][j])
			require.NoError(t, err)
			require.Equal(t, values[i][j], val)
		}
	}
}

func TestLoadVersion(t *testing.T) {
	tree, closer := setupFullTreeP(t, 0)
	defer closer()

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
		tree := NewTree(tree.store)
		require.NoError(t, tree.LoadVersion(uint64(i+1)))
		for j := range added[i] {
			val, err := tree.Get(added[i][j])
			require.NoError(t, err)
			require.Equal(t, values[i][j], val)
		}
	}
}

func TestDelete(t *testing.T) {
	tree := setupFullTree(t, 0)
	keys := [][]byte{}

	for i := 0; i < 10; i++ {
		key := make([]byte, 10)
		rand.Read(key)
		require.NoError(t, tree.Put(key, key))
		keys = append(keys, key)
	}
	require.NoError(t, tree.Commit())

	for _, key := range keys {
		require.NoError(t, tree.Delete(key))
	}
	require.NoError(t, tree.Commit())

	for _, key := range keys {
		value, err := tree.Get(key)
		require.Nil(t, value)
		require.True(t, errors.Is(err, ErrNotFound))
	}

	version := tree.Version()
	require.NoError(t, tree.LoadVersion(version-1))

	for _, key := range keys {
		value, err := tree.Get(key)
		require.NoError(t, err)
		require.Equal(t, key, value)
	}
}

func TestOpenExistingStore(t *testing.T) {

}

func BenchmarkRandomRead500000(b *testing.B) {
	tree, closer := setupProdTree(b)
	defer closer()

	for i := 0; i < 500000; i++ {
		key := make([]byte, 10)
		value := make([]byte, 50)
		rand.Read(key)
		rand.Read(value)
		require.NoError(b, tree.Put(key, value))
	}
	require.NoError(b, tree.Commit())
	key := make([]byte, 10)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rand.Read(key)
		_, _ = tree.Snapshot().Get(key)
	}
}

type testTree interface {
	Put([]byte, []byte) error
	Commit() error
}

func benchmarkCommitPersistent(b *testing.B, tree testTree, db *store.FileStore, commit int) {
	memory := make([]uint64, b.N)
	spent := make([]time.Duration, b.N)
	total := make([]int, b.N)
	count := 0
	stats := &runtime.MemStats{}
	runtime.ReadMemStats(stats)
	dbstats := store.Stats{}
	alloc := stats.Alloc
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		for j := 0; j < commit; j++ {
			key := make([]byte, 10)
			rand.Read(key)
			require.NoError(b, tree.Put(key, key))
		}
		require.NoError(b, tree.Commit())

		runtime.ReadMemStats(stats)
		memory[i] = stats.Alloc - alloc
		spent[i] = time.Since(start)
		count += commit
		total[i] = count
		db.ReadStats(&dbstats)
		log.Printf("time spent %v, total %d, tree flush %d, util %f", spent[i], total[i], dbstats.Tree.MeanFlushSize, dbstats.Tree.FlushUtilization)
	}
	if b.N > 1 {
		require.NoError(b, utils.PlotTimeSpent(spent, total, fmt.Sprintf("_assets/time-spent-commit-%d-%d", commit, count)))
		require.NoError(b, utils.PlotMemory(memory, total, fmt.Sprintf("_assets/memory-alloc-commit-%d-%d", commit, count)))
	}
}

func BenchmarkCommitPersistent5000Entries(b *testing.B) {
	tree, closer := setupProdTree(b)
	defer closer()
	benchmarkCommitPersistent(b, tree, tree.store, 5000)
}

func BenchmarkCommitPersistent40000Entries(b *testing.B) {
	tree, closer := setupProdTree(b)
	defer closer()
	benchmarkCommitPersistent(b, tree, tree.store, 40000)
}

func BenchmarkPeriodicWrites500Commit40000(b *testing.B) {
	tree, closer := setupProdTree(b)
	defer closer()
	benchmarkCommitPersistent(b, NewFlushTreeFromTree(tree, 500), tree.store, 40000)
}

func BenchmarkPeriodicWrites2000Commit44000(b *testing.B) {
	tree, closer := setupProdTree(b)
	defer closer()
	benchmarkCommitPersistent(b, NewFlushTreeFromTree(tree, 2000), tree.store, 44000)
}

func BenchmarkPeriodicWrites500Commit5000(b *testing.B) {
	tree, closer := setupProdTree(b)
	defer closer()
	benchmarkCommitPersistent(b, NewFlushTreeFromTree(tree, 500), tree.store, 5000)
}

func BenchmarkPeriodicWrites1000Commit5000(b *testing.B) {
	tree, closer := setupProdTree(b)
	defer closer()
	benchmarkCommitPersistent(b, NewFlushTreeFromTree(tree, 1000), tree.store, 5000)
}

func BenchmarkBlock100(b *testing.B) {
	tree, closer := setupProdTree(b)
	defer closer()
	benchmarkCommitPersistent(b, tree, tree.store, 100)
}

func BenchmarkBlock500(b *testing.B) {
	tree, closer := setupProdTree(b)
	defer closer()
	benchmarkCommitPersistent(b, tree, tree.store, 500)
}

func BenchmarkBlock5000(b *testing.B) {
	tree, closer := setupProdTree(b)
	defer closer()
	benchmarkCommitPersistent(b, tree, tree.store, 5000)
}

func BenchmarkBlock10000(b *testing.B) {
	tree, closer := setupProdTree(b)
	defer closer()
	benchmarkCommitPersistent(b, tree, tree.store, 10000)
}
