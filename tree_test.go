package urkeltrie

import (
	"bytes"
	"errors"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/dshulyak/urkeltrie/store"
	"github.com/stretchr/testify/require"
)

func setupProdTree(tb testing.TB) (*Tree, func()) {
	tmp, err := ioutil.TempDir("", "testing-prod-urkel")
	require.NoError(tb, err)
	store, err := store.Open(store.DefaultConfig(tmp))
	require.NoError(tb, err)
	return NewTree(store), func() { require.NoError(tb, os.RemoveAll(tmp)) }
}

func setupTreeSmall(tb testing.TB) (*Tree, func()) {
	tmp, err := ioutil.TempDir("", "testing-urkel")
	require.NoError(tb, err)
	conf := store.DefaultConfig(tmp)
	conf.MaxFileSize = 4096
	store, err := store.Open(conf)
	require.NoError(tb, err)
	return NewTree(store), func() { require.NoError(tb, os.RemoveAll(tmp)) }
}

func setupFullTree(tb testing.TB, hint int) *Tree {
	store, err := store.Open(store.DefaultConfig(""))
	require.NoError(tb, err)
	tree := NewTree(store)
	for i := 0; i < hint; i++ {
		key := make([]byte, 20)
		value := make([]byte, 10)
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
	store, err := store.Open(store.DefaultConfig(tmp))
	require.NoError(tb, err)

	tree := NewTree(store)

	for i := 0; i < hint; i++ {
		key := make([]byte, 20)
		value := make([]byte, 10)
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
	tree := setupFullTree(t, 0)
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
	tree, closer := setupTreeSmall(t)
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

func TestTreeSnapshotConcurrentRead(t *testing.T) {
	tree, closer := setupFullTreeP(t, 0)
	defer closer()

	var (
		keys   = [][]byte{}
		values = [][]byte{}
		wg     sync.WaitGroup
	)
	for i := 0; i < 1000; i++ {
		key := make([]byte, 10)
		rand.Read(key)
		value := make([]byte, 40)
		rand.Read(value)

		require.NoError(t, tree.Put(key, value))

		keys = append(keys, key)
		values = append(values, value)
	}

	require.NoError(t, tree.Commit())

	wg.Add(len(keys))
	for i := range keys {
		key := keys[i]
		value := values[i]
		go func() {
			defer wg.Done()
			rst, err := tree.Snapshot().Get(key)
			require.NoError(t, err)
			require.Equal(t, value, rst)
		}()
	}
	wg.Wait()
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

func TestUpdate(t *testing.T) {
	tree := setupFullTree(t, 0)

	key := make([]byte, 10)
	rand.Read(key)
	require.NoError(t, tree.Put(key, []byte{1, 2, 3}))

	update := []byte{3, 3, 3}
	require.NoError(t, tree.Put(key, update))
	val, err := tree.Get(key)
	require.NoError(t, err)
	require.Equal(t, update, val)
}

func TestUpdateCommited(t *testing.T) {
	tree := setupFullTree(t, 0)

	key := make([]byte, 10)
	rand.Read(key)
	require.NoError(t, tree.Put(key, []byte{1, 2, 3}))
	require.NoError(t, tree.Commit())

	update := []byte{3, 3, 3}
	require.NoError(t, tree.Put(key, update))
	val, err := tree.Get(key)
	require.NoError(t, err)
	require.Equal(t, update, val)
}

func TestDelete(t *testing.T) {
	rand.Seed(time.Now().Unix())
	tree, closer := setupFullTreeP(t, 0)
	defer closer()
	keys := [][]byte{}

	for i := 0; i < 1000; i++ {
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
		require.True(t, errors.Is(err, ErrNotFound), "error is %v", err)
	}

	version := tree.Version()
	require.NoError(t, tree.LoadVersion(version-1))

	for _, key := range keys {
		value, err := tree.Get(key)
		require.NoError(t, err)
		require.Equal(t, key, value)
	}
}

func TestReadDirtyDeleted(t *testing.T) {
	rand.Seed(time.Now().Unix())
	tree, closer := setupFullTreeP(t, 100)
	defer closer()

	keys := [][]byte{}

	for i := 0; i < 10; i++ {
		key := make([]byte, 10)
		rand.Read(key)
		require.NoError(t, tree.Put(key, key))
		keys = append(keys, key)
	}
	for i := range keys {
		require.NoError(t, tree.Delete(keys[i]))
		val, err := tree.Get(keys[i])
		require.Nil(t, val)
		require.Error(t, err)
	}
}

func TestReadDelete(t *testing.T) {
	rand.Seed(time.Now().Unix())
	tree, closer := setupFullTreeP(t, 0)
	defer closer()

	keys := [][]byte{}

	for i := 0; i < 10; i++ {
		key := make([]byte, 10)
		rand.Read(key)
		require.NoError(t, tree.Put(key, key))
		keys = append(keys, key)
	}

	require.NoError(t, tree.Commit())
	require.NoError(t, tree.Delete(keys[0]))
	_, err := tree.Get(keys[2])
	require.NoError(t, err)
	_, err = tree.Get(keys[0])
	require.Error(t, err)
}

func TestTreeIterate(t *testing.T) {
	tree := setupFullTree(t, 0)

	keys := [][]byte{}
	for i := 0; i < 10; i++ {
		key := make([]byte, 10)
		rand.Read(key)
		require.NoError(t, tree.Put(key, key))
		keys = append(keys, key)
	}
	require.NoError(t, tree.Commit())

	sort.Slice(keys, func(i, j int) bool {
		return bytes.Compare(keys[i], keys[j]) == -1
	})

	rst := [][]byte{}
	require.NoError(t, tree.Iterate(func(e Entry) bool {
		key, err := e.Key()
		require.NoError(t, err)
		rst = append(rst, key)
		return false
	}))

	sort.Slice(rst, func(i, j int) bool {
		return bytes.Compare(rst[i], rst[j]) == -1
	})

	require.Equal(t, keys, rst)
}

func TestTreeIterateDirty(t *testing.T) {
	tree := setupFullTree(t, 0)

	keys := [][]byte{}
	for i := 0; i < 10; i++ {
		key := make([]byte, 10)
		rand.Read(key)
		require.NoError(t, tree.Put(key, key))
		keys = append(keys, key)
	}

	rst := [][]byte{}
	require.NoError(t, tree.Iterate(func(e Entry) bool {
		key, err := e.Key()
		require.NoError(t, err)
		rst = append(rst, key)
		return false
	}))
	require.Len(t, rst, len(keys))
}

func TestTreeIterateReverse(t *testing.T) {
	tree := setupFullTree(t, 0)

	keys := [][]byte{}
	for i := 0; i < 10; i++ {
		key := make([]byte, 10)
		rand.Read(key)
		require.NoError(t, tree.Put(key, key))
		keys = append(keys, key)
	}

	inorder := [][]byte{}
	require.NoError(t, tree.Iterate(func(e Entry) bool {
		key, err := e.Key()
		require.NoError(t, err)
		inorder = append(inorder, key)
		return false
	}))

	reverse := [][]byte{}
	require.NoError(t, tree.ReverseIterate(func(e Entry) bool {
		key, err := e.Key()
		require.NoError(t, err)
		reverse = append(reverse, key)
		return false
	}))

	last := len(inorder) - 1
	for i := range inorder {
		require.Equal(t, inorder[i], reverse[last-i])
	}
}

func TestOpenExistingStore(t *testing.T) {
	tmp, err := ioutil.TempDir("", "testing-open-file-store-")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(tmp)) }()

	st1, err := store.Open(store.DefaultConfig(tmp))
	require.NoError(t, err)

	var (
		keys = [][]byte{}
		tree = NewTree(st1)
	)
	for i := 0; i < 10; i++ {
		key := make([]byte, 10)
		rand.Read(key)
		require.NoError(t, tree.Put(key, key))
	}
	require.NoError(t, tree.Commit())

	st2, err := store.Open(store.DefaultConfig(tmp))
	require.NoError(t, err)
	tree = NewTree(st2)
	require.NoError(t, tree.LoadLatest())

	for _, key := range keys {
		val, err := tree.Get(key)
		require.NoError(t, err)
		require.Equal(t, key, val)
	}

	keys = keys[:0]
	for i := 0; i < 10; i++ {
		key := make([]byte, 10)
		rand.Read(key)
		require.NoError(t, tree.Put(key, key))
	}
	require.NoError(t, tree.Commit())

}

func TestConsistentState(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping long test")
		return
	}
	tmp, err := ioutil.TempDir("", "testing-consistent-state-")
	require.NoError(t, err)
	defer func() { require.NoError(t, os.RemoveAll(tmp)) }()

	st, err := store.Open(store.DefaultConfig(tmp))
	require.NoError(t, err)
	tree := NewTree(st)

	var (
		versions = 50
		entries  = 100
		state    = make([]map[string][]byte, versions)
	)
	for i := 0; i < versions; i++ {
		vstate := make(map[string][]byte, entries)
		state[i] = vstate
		for j := 0; j < entries; j++ {
			key := make([]byte, 30)
			value := make([]byte, 10)
			rand.Read(key)
			rand.Read(value)
			require.NoError(t, tree.Put(key, value))
			vstate[string(key)] = value
		}
		require.NoError(t, tree.Commit())
	}

	require.NoError(t, st.Close())

	st, err = store.Open(store.DefaultConfig(tmp))
	require.NoError(t, err)
	tree = NewTree(st)
	require.NoError(t, tree.LoadLatest())

	// test that latest version has all records
	for i := 0; i < versions; i++ {
		for key, expect := range state[i] {
			value, err := tree.Get([]byte(key))
			require.NoError(t, err)
			require.Equal(t, expect, value)
		}
	}

	// test that older versions doesn't have newer records, and has all older records
	for i := 0; i < versions; i++ {
		snap, err := tree.VersionSnapshot(uint64(i + 1))
		require.NoError(t, err)

		for a := 0; a <= i; a++ {
			for key, expect := range state[a] {
				value, err := snap.Get([]byte(key))
				require.NoError(t, err)
				require.NotNil(t, value)
				require.Equal(t, expect, value)
			}
		}

		for j := i + 1; j < versions; j++ {
			for key := range state[j] {
				value, err := snap.Get([]byte(key))
				require.Nil(t, value)
				require.True(t, errors.Is(err, ErrNotFound), "error is %v", err)
			}
		}
	}
}

func BenchmarkRandomRead500000(b *testing.B) {
	tree, closer := setupProdTree(b)
	defer closer()

	size := 500000
	keys := make([][]byte, 0, size)
	for i := 0; i < size; i++ {
		key := make([]byte, 10)
		value := make([]byte, 50)
		rand.Read(key)
		rand.Read(value)
		require.NoError(b, tree.Put(key, value))
		keys = append(keys, key)
	}
	require.NoError(b, tree.Commit())
	b.ResetTimer()
	b.SetBytes(1 << 20)
	for i := 0; i < b.N; i++ {
		idx := rand.Intn(size)
		key := keys[idx]
		_, _ = tree.Get(key)
	}
}

type testTree interface {
	Put([]byte, []byte) error
	Commit() error
}

func benchmarkCommitPersistent(b *testing.B, tree testTree, db *store.FileStore, commit int) {
	count := 0
	stats := &runtime.MemStats{}
	runtime.ReadMemStats(stats)
	alloc := stats.Alloc

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		for j := 0; j < commit; j++ {
			key := make([]byte, 30)
			value := make([]byte, 100)
			rand.Read(key)
			rand.Read(value)
			require.NoError(b, tree.Put(key, value))
		}
		require.NoError(b, tree.Commit())

		count += commit
		runtime.ReadMemStats(stats)
		log.Printf("time spent %v, total %d, mem %d", time.Since(start), count, stats.Alloc-alloc)
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

func BenchmarkBlock1000(b *testing.B) {
	tree, closer := setupProdTree(b)
	defer closer()
	benchmarkCommitPersistent(b, tree, tree.store, 1000)
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

func BenchmarkBlock40000(b *testing.B) {
	tree, closer := setupProdTree(b)
	defer closer()
	benchmarkCommitPersistent(b, tree, tree.store, 40000)
}
