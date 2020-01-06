package urkeltrie

import (
	"sync"
)

func newIterator(tree TreeIterator, reverse bool) *Iterator {
	mu := new(sync.Mutex)
	iter := &Iterator{
		tree:    tree,
		mu:      mu,
		reader:  sync.NewCond(mu),
		writer:  sync.NewCond(mu),
		reverse: reverse,
	}
	iter.init()
	return iter
}

func NewReverseIterator(tree TreeIterator) *Iterator {
	return newIterator(tree, true)
}

func NewIterator(tree TreeIterator) *Iterator {
	return newIterator(tree, false)
}

type Iterator struct {
	tree    TreeIterator
	reverse bool

	mu     *sync.Mutex
	reader *sync.Cond
	writer *sync.Cond

	key, value []byte
	closed     bool
	err        error
}

func (i *Iterator) init() {
	if i.tree == nil {
		i.closed = true
		return
	}
	i.mu.Lock()
	go func() {
		treeIter := i.tree.Iterate
		if i.reverse {
			treeIter = i.tree.ReverseIterate
		}
		err := treeIter(func(e Entry) bool {
			key, err := e.Key()
			if err != nil {
				i.err = err
				return true
			}
			value, err := e.Value()
			if err != nil {
				i.err = err
				return true
			}

			i.mu.Lock()
			i.key = key
			i.value = value
			i.reader.Signal()
			i.writer.Wait()
			closed := i.closed
			i.mu.Unlock()
			return closed
		})
		i.mu.Lock()
		i.closed = true
		i.err = err
		i.mu.Unlock()
		i.reader.Signal()
	}()
	i.reader.Wait()
	i.mu.Unlock()
}

// Next notifies iterator routine that it should proceed and blocks until iterator notifies
// about step completetion.
func (i *Iterator) Next() {
	if !i.Valid() {
		panic("iterator is invalid")
	}
	i.mu.Lock()
	i.writer.Signal()
	i.reader.Wait()
	i.mu.Unlock()
}

func (i *Iterator) Valid() bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.err == nil && !i.closed
}

func (i *Iterator) Key() []byte {
	return i.key
}

func (i *Iterator) Value() []byte {
	return i.value
}

func (i *Iterator) Close() {
	if !i.Valid() {
		return
	}
	i.closed = true
	i.writer.Signal()
}

func (i *Iterator) Error() error {
	return i.err
}
