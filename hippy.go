package hippy

import (
	"bufio"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	_none byte = iota

	_put // Byte representing a PUT action
	_del // Byte representing a DELETE action

	_separator = ':'  // Separator used to split key and value
	_newline   = '\n' // Character for newline
	_pound     = '#'  // Character for pound
	_space     = ' '  // Character for space
)

const (
	// ErrInvalidAction is returned when an invalid action occurs
	ErrInvalidAction = Error("invalid action")

	// ErrInvalidKey is returned when an invalid key is provided
	ErrInvalidKey = Error("invalid key")

	// ErrHashLine is returned when a hash line is parsed as a log line
	ErrHashLine = Error("cannot parse hash line as log line")

	// ErrHashNotFound is returned when a hash line is not found while archiving
	ErrHashNotFound = Error("hash not found")

	// ErrIsClosed is returned when an action is attempted on a closed instance
	ErrIsClosed = Error("cannot perform action on closed instance")
)

// New returns a new Hippy
func New(path, name string, mws ...Middleware) (h *Hippy, err error) {
	hip := Hippy{
		// Make the internal storage map, it would be a shame to panic on put!
		s: make(storage),

		path: path,
		name: name,
		mws:  mws,
	}

	// Open persistance file
	if hip.f, err = os.OpenFile(filepath.Join(path, name+".hdb"), os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644); err != nil {
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

	path string       // Database path
	name string       // Database name
	mws  []Middleware // Middlewares

	closed bool // Closed state
}

func (h *Hippy) replay() (err error) {
	var (
		a   byte
		key string
		val []byte
		de  bool // Data exists boolean

		rdr  io.ReadCloser
		scnr *bufio.Scanner

		hasMW = len(h.mws) > 0
	)

	h.mux.Lock()
	if !hasMW {
		rdr = h.f
	} else {
		if rdr, err = newMWReader(h.f, h.mws); err != nil {
			if err == io.EOF {
				err = nil
			}
			goto END
		}
	}

	scnr = bufio.NewScanner(rdr)
	// For each line..
	for scnr.Scan() {
		// Parse action, key, and value
		if a, key, val, err = parseLogLine(scnr.Bytes()); err != nil {
			if err == ErrHashLine {
				de = true
			}

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

		// We know data exists, let's set de to true
		de = true
	}

	if hasMW {
		// We only close our reader if it's a middleware reader
		rdr.Close()
	}

END:
	if !de {
		if !hasMW {
			h.f.Write(newHashLine())
		} else {
			var w io.WriteCloser
			if w, err = newMWWriter(h.f, h.mws); err != nil {
				goto END
			}

			hl := newHashLine()
			w.Write(hl)
			w.Close()
		}

		h.f.Sync()
	}

	h.mux.Unlock()
	return
}

// write will write a transaction to disk
// Note: This is not thread safe. It is expected that the calling function is managing locks
func (h *Hippy) write(a map[string]action) (err error) {
	var (
		w     io.WriteCloser
		hasMW = len(h.mws) > 0
	)

	if !hasMW {
		w = h.f
	} else {
		if w, err = newMWWriter(h.f, h.mws); err != nil {
			return
		}
	}

	for k, v := range a {
		// We are going to write before modifying memory
		if _, err = w.Write(newLogLine(k, v.a, v.b)); err != nil {
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

	if hasMW {
		// We only close our writer if it's a middleware writer
		w.Close()
	}

	// Call sync to make sure the data has flushed to disk
	return h.f.Sync()
}

func (h *Hippy) compact() (err error) {
	var (
		archv *os.File
		tmp   *os.File
		fi    os.FileInfo
		hash  []byte

		tmpW  io.WriteCloser
		hasMW = len(h.mws) > 0

		fLoc = filepath.Join(h.path, h.name+".hdb")         // Main file location
		aLoc = filepath.Join(h.path, h.name+".archive.hdb") // Archive file location
		tLoc = filepath.Join(h.path, h.name+".tmp.hdb")     // Temporary file location
	)

	h.mux.Lock()
	// Open archive file
	if archv, err = os.OpenFile(aLoc, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644); err != nil {
		goto END
	}

	if tmp, err = os.OpenFile(tLoc, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		goto END
	}

	if !hasMW {
		tmpW = tmp
	} else {
		if tmpW, err = newMWWriter(tmp, h.mws); err != nil {
			goto END
		}
	}

	if hash, err = archive(h.f, archv, h.mws); err != nil {
		goto END
	}

	// Write data contents to tmp file
	for k, v := range h.s {
		if _, err = tmpW.Write(newLogLine(k, _put, v)); err != nil {
			goto END
		}
	}

	if err = archv.Close(); err != nil {
		goto END
	}

	// Add our current hash to the end
	if _, err = tmpW.Write(hash); err != nil {
		goto END
	}

	if hasMW {
		tmpW.Close()
	}

	if err = tmp.Close(); err != nil {
		goto END
	}

	if err = h.f.Close(); err != nil {
		goto END
	}

	err = os.Rename(tLoc, fLoc)

	// Open persistance file
	if h.f, err = os.OpenFile(fLoc, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644); err != nil {
		goto END
	}

	if fi, err = h.f.Stat(); err != nil {
		goto END
	}

	h.f.Seek(fi.Size(), 0)

END:
	h.mux.Unlock()
	return
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
	if h.closed {
		err = ErrIsClosed
	} else {
		err = fn(tx)
	}
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
	if h.closed {
		err = ErrIsClosed
		goto END
	}

	if err = fn(tx); err == nil {
		err = h.write(tx.a)
	}

END:
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
	if h.closed {
		err = ErrIsClosed
		goto END
	}

	if err = fn(tx); err == nil {
		err = h.write(tx.a)
	}

END:
	h.mux.Unlock()
	// Return write transaction to the pool
	h.putWriteTx(tx)
	return
}

// Close will close Hippyh
func (h *Hippy) Close() (err error) {
	h.mux.Lock()
	if h.closed {
		err = ErrIsClosed
	} else {
		h.closed = true
	}
	h.mux.Unlock()

	if err != nil {
		return
	}

	return h.compact()
}
