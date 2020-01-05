package urkeltrie

import (
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

type genOp func(tb testing.TB, state *state) testOp

type testOp interface {
	Run(tree *Tree)
	String() string
}

func genDel(tb testing.TB, state *state) testOp {
	key := state.randEntry()
	if key == nil {
		return new(noop)
	}
	return &del{
		tb:    tb,
		state: state,
		key:   key,
	}
}

type noop struct{}

func (n *noop) Run(tree *Tree) {}

func (n *noop) String() string {
	return "NOOP"
}

type del struct {
	tb    testing.TB
	state *state
	key   []byte
}

func (del *del) String() string {
	return fmt.Sprintf("Delete %x", del.key)
}

func (d *del) Run(tree *Tree) {
	require.NoError(d.tb, tree.Delete(d.key))
	d.state.delete(d.key)
}

func genUpdate(tb testing.TB, state *state) testOp {
	key := make([]byte, 10)
	value := make([]byte, 50)
	rand.Read(key)
	rand.Read(value)
	return &update{
		tb:    tb,
		state: state,
		key:   key,
		value: value,
	}
}

type update struct {
	tb         testing.TB
	state      *state
	key, value []byte
}

func (u *update) String() string {
	return fmt.Sprintf("Update %x = %x", u.key, u.value[:20])
}

func (u *update) Run(tree *Tree) {
	require.NoError(u.tb, tree.Put(u.key, u.value))
	u.state.put(u.key, u.value)
}

func genGet(tb testing.TB, state *state) testOp {
	key := state.randEntry()
	if key == nil {
		return new(noop)
	}
	return &get{
		tb:    tb,
		state: state,
		key:   key,
	}
}

type get struct {
	tb    testing.TB
	state *state
	key   []byte
}

func (g *get) String() string {
	return fmt.Sprintf("Get %x", g.key)
}

func (g *get) Run(tree *Tree) {
	val, err := tree.Get(g.key)
	require.NoError(g.tb, err)
	require.Equal(g.tb, g.state.get(g.key), val)
}

func genNotfound(tb testing.TB, state *state) testOp {
	key := state.randDeleted()
	if key == nil {
		return new(noop)
	}
	return &notfound{
		tb:    tb,
		state: state,
		key:   key,
	}
}

type notfound struct {
	tb    testing.TB
	state *state
	key   []byte
}

func (nf *notfound) String() string {
	return fmt.Sprintf("Not Found %x", nf.key)
}

func (nf *notfound) Run(tree *Tree) {
	val, err := tree.Get(nf.key)
	require.Nil(nf.tb, val)
	require.True(nf.tb, errors.Is(err, ErrNotFound), "error is %v", err)
}

func genCommit(tb testing.TB, state *state) testOp {
	return &commit{tb: tb}
}

type commit struct {
	tb testing.TB
}

func (c *commit) String() string {
	return "Commit"
}

func (c *commit) Run(tree *Tree) {
	require.NoError(c.tb, tree.Commit())
}

func newState() *state {
	return &state{
		entries: map[string][]byte{},
		deleted: map[string]struct{}{},
	}
}

type state struct {
	entries map[string][]byte
	deleted map[string]struct{}
}

func (s *state) put(key, value []byte) {
	s.entries[string(key)] = value
	delete(s.deleted, string(key))
}

func (s *state) get(key []byte) []byte {
	return s.entries[string(key)]
}

func (s *state) delete(key []byte) {
	delete(s.entries, string(key))
	s.deleted[string(key)] = struct{}{}
}

func (s *state) randEntry() []byte {
	if len(s.entries) == 0 {
		return nil
	}
	array := make([]string, 0, len(s.entries))
	for key := range s.entries {
		array = append(array, key)
	}
	sort.Slice(array, func(i, j int) bool {
		return array[i] < array[j]
	})
	return []byte(array[rand.Intn(len(array))])
}

func (s *state) randDeleted() []byte {
	if len(s.deleted) == 0 {
		return nil
	}
	array := make([]string, 0, len(s.deleted))
	for key := range s.deleted {
		array = append(array, key)
	}
	sort.Slice(array, func(i, j int) bool {
		return array[i] < array[j]
	})
	return []byte(array[rand.Intn(len(array))])
}

func TestFuzzTree(t *testing.T) {
	seed := time.Now().UnixNano()
	rand.Seed(seed)
	t.Logf("fuzz using seed: %d", seed)

	// TODO add weights
	gens := []genOp{genUpdate, genGet, genNotfound, genDel, genCommit}
	state := newState()

	tree, closer := setupFullTreeP(t, 0)
	defer closer()

	for i := 0; i < 1000; i++ {
		index := rand.Intn(len(gens))
		op := gens[index](t, state)
		op.Run(tree)
	}
}
