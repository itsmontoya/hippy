package hippy

import (
	"bufio"
	"errors"
	"os"
	"sync"
)

const (
	_none byte = iota

	_put // Byte representing a PUT action
	_del // Byte representing a DELETE action

	_separator = ':'  // Separator used to split key and value
	_newline   = '\n' // Character for newline
)

var (
	// ErrInvalidAction is returned when an invalid action occurs
	ErrInvalidAction = errors.New("invalid action")

	// ErrInvalidKey is returned when an invalid key is provided
	ErrInvalidKey = errors.New("invalid key")
)

// New returns a new Hippy
func New(loc string) (h *Hippy, err error) {
	hip := Hippy{
		// Make the internal storage map, it would be a shame to panic on put!
		s: make(storage),
	}

	// Open persistance file
	if hip.f, err = os.OpenFile(loc, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644); err != nil {
		return
	}

	h = &hip
	// Initialize transaction pools
	h.rtxp = sync.Pool{New: func() interface{} { return h.newReadTx() }}
	h.wtxp = sync.Pool{New: func() interface{} { return h.newWriteTx() }}
	h.rwtxp = sync.Pool{New: func() interface{} { return h.newReadWriteTx() }}

	// Replay file data to populate the database
	h.replay()
	return
}

// Hippy is a db
type Hippy struct {
	mux sync.RWMutex

	s storage  // In-memory storage
	f *os.File // Persistant storage

	rtxp  sync.Pool // Read transaction pool
	wtxp  sync.Pool // Write transaction pool
	rwtxp sync.Pool // Read/Write transaction pool

	closed bool // Closed state
}

func (h *Hippy) replay() {
	var (
		a   byte
		key string
		val []byte
		err error
	)

	h.mux.Lock()
	scnr := bufio.NewScanner(h.f)
	// For each line..
	for scnr.Scan() {
		// Parse action, key, and value
		if a, key, val, err = parseLogLine(scnr.Bytes()); err != nil {
			continue
		}

		// Fulfill action
		switch a {
		case _put:
			// Put value by key
			h.s[key] = val
		case _del:
			// Delete by key
			delete(h.s, key)
		}
	}
	h.mux.Unlock()
}

func (h *Hippy) write(a map[string]action) (err error) {
	for k, v := range a {
		// We are going to write before modifying memory
		if _, err = h.f.Write(newLogLine(k, v.a, v.b)); err != nil {
			return
		}

		// Fulfill action
		switch v.a {
		case _put:
			// Put by key
			h.s[k] = v.b
		case _del:
			// Delete by key
			delete(h.s, k)
		}
	}

	// Call sync to make sure the data has flushed to disk
	return h.f.Sync()
}

// newReadTx returns a new read transaction, used by read transaction pool
func (h *Hippy) newReadTx() *ReadTx {
	return &ReadTx{&h.s}
}

// newWriteTx returns a new write transaction, used by write transaction pool
func (h *Hippy) newWriteTx() *WriteTx {
	return &WriteTx{
		a: make(map[string]action),
	}
}

// newReadWriteTx returns a new read/write transaction, used by read/write transaction pool
func (h *Hippy) newReadWriteTx() *ReadWriteTx {
	return &ReadWriteTx{
		s: &h.s,
		a: make(map[string]action),
	}
}

// getReadTx returns a new read transaction from the read transaction pool
func (h *Hippy) getReadTx() (tx *ReadTx) {
	tx, _ = h.rtxp.Get().(*ReadTx)
	return
}

// getWriteTx returns a new write transaction from the write transaction pool
func (h *Hippy) getWriteTx() (tx *WriteTx) {
	tx, _ = h.wtxp.Get().(*WriteTx)
	return
}

// getReadWriteTx returns a new read/write transaction from the read/write transaction pool
func (h *Hippy) getReadWriteTx() (tx *ReadWriteTx) {
	tx, _ = h.rwtxp.Get().(*ReadWriteTx)
	return
}

// putReadTx releases a read transaction back to the read transaction pool
func (h *Hippy) putReadTx(tx *ReadTx) {
	h.rtxp.Put(tx)
}

// putWriteTx releases a write transaction back to the write transaction pool
func (h *Hippy) putWriteTx(tx *WriteTx) {
	for k := range tx.a {
		delete(tx.a, k)
	}

	h.wtxp.Put(tx)
}

// putReadWriteTx releases a read/write transaction back to the read/write transaction pool
func (h *Hippy) putReadWriteTx(tx *ReadWriteTx) {
	for k := range tx.a {
		delete(tx.a, k)
	}

	h.rwtxp.Put(tx)
}

// Read will return a read-only transaction
func (h *Hippy) Read(fn func(*ReadTx) error) (err error) {
	// Get a read transaction from the pool
	tx := h.getReadTx()

	h.mux.RLock()
	err = fn(tx)
	h.mux.RUnlock()

	// Return read transaction to the pool
	h.putReadTx(tx)
	return
}

// ReadWrite returns a read/write transaction
func (h *Hippy) ReadWrite(fn func(*ReadWriteTx) error) (err error) {
	// Get a read/write transaction from the pool
	tx := h.getReadWriteTx()

	h.mux.Lock()
	if err = fn(tx); err == nil {
		err = h.write(tx.a)
	}
	h.mux.Unlock()

	// Return read/write transaction to the pool
	h.putReadWriteTx(tx)
	return
}

// Write returns a write-only transaction
func (h *Hippy) Write(fn func(*WriteTx) error) (err error) {
	// Get a write transaction from the pool
	tx := h.getWriteTx()

	h.mux.Lock()
	if err = fn(tx); err == nil {
		err = h.write(tx.a)
	}
	h.mux.Unlock()

	// Return write transaction to the pool
	h.putWriteTx(tx)
	return
}
