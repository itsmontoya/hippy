package hippy

import (
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	_none byte = iota

	_put  // Byte representing a PUT action
	_del  // Byte representing a DELETE action
	_hash // Hash line

	_separator = ':'  // Separator used to split key and value
	_newline   = '\n' // Character for newline
	_pound     = '#'  // Character for pound
	_space     = ' '  // Character for space

	// MaxKeyLen is the maximum length for keys
	MaxKeyLen = 255
	hashLen   = 16
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

	// ErrIsOpen is returned when an instance is attempted to be re-opened when it's already active
	ErrIsOpen = Error("cannot open an instance which is already open")
)

// New returns a new Hippy
func New(path, name string, opts Opts, mws ...Middleware) (h *Hippy, err error) {
	hip := Hippy{
		// Make the internal storage map, it would be a shame to panic on put!
		s: make(storage),

		fLoc:  filepath.Join(path, name+".hdb"),
		tfLoc: filepath.Join(path, name+".tmp.hdb"),

		path: path,
		name: name,
		mws:  mws,
		opts: opts,
	}

	mws = append(mws, b64MW{})
	reverseMWSlice(mws)

	// Open persistance file
	if hip.f, err = newFile(path, name, mws, true); err != nil {
		return
	}

	// Open archive file
	if hip.af, err = newFile(path, name+".archive", mws, true); err != nil {
		return
	}

	// Open temporary file
	if hip.tf, err = newFile(path, name+".tmp", mws, false); err != nil {
		return
	}

	h = &hip
	// Initialize transaction pools
	h.rtxp = sync.Pool{New: func() interface{} { return h.newReadTx() }}
	h.wtxp = sync.Pool{New: func() interface{} { return h.newWriteTx() }}
	h.rwtxp = sync.Pool{New: func() interface{} { return h.newReadWriteTx() }}

	// Replay file data to populate the database
	err = h.f.Read(h.replay)
	return
}

// Hippy is a db
type Hippy struct {
	mux sync.RWMutex

	s storage // In-memory storage

	fLoc  string
	tfLoc string

	f  *file // Persistent storage
	af *file // Archive file
	tf *file // Temporary file

	rtxp  sync.Pool // Read transaction pool
	wtxp  sync.Pool // Write transaction pool
	rwtxp sync.Pool // Read/Write transaction pool

	path string // Database path
	name string // Database name
	mws  []Middleware
	opts Opts

	closed bool // Closed state
}

func (h *Hippy) replay(fr *fileReader) (err error) {
	var (
		a   byte
		key string
		val []byte
		de  bool // Data exists boolean
	)

	h.mux.Lock()
	// For each line..
	for b, err := fr.ReadLine(); err == nil; b, err = fr.ReadLine() {
		// Parse action, key, and value
		if a, key, val, err = parseLogLine(b); err != nil {
			continue
		}

		// Fulfill action
		switch a {
		case _hash:
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

	if de || (err != nil && err != io.EOF) {
		goto END
	}

	err = h.f.WriteLine(newHashLine())

END:
	h.mux.Unlock()
	return
}

// write will write a transaction to disk
// Note: This is not thread safe. It is expected that the calling function is managing locks
func (h *Hippy) write(a map[string]action) (err error) {
	for k, v := range a {
		// We are going to write before modifying memory
		if err = h.f.WriteLine(newLogLine(v.a, k, v.b)); err != nil {
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

	return h.f.Flush()
}

func (h *Hippy) archive(fr *fileReader) (err error) {
	var nd bool // New data boolean

	if err = h.af.SeekToEnd(); err != nil {
		return
	}

	for b, err := fr.ReadLine(); err == nil; b, err = fr.ReadLine() {
		// Parse action
		a, _, _, _ := parseLogLine(b)
		switch a {
		case _put, _del:
			h.af.WriteLine(b)
			nd = true
		}
	}

	if !nd {
		return
	}

	if err = h.af.WriteLine(newHashLine()); err != nil {
		return
	}

	return h.af.Flush()
}

func (h *Hippy) compact() (err error) {
	if err = h.tf.SetFile(); err != nil {
		return
	}

	// Write data contents to tmp file
	for k, v := range h.s {
		if err = h.tf.WriteLine(newLogLine(_put, k, v)); err != nil {
			return
		}
	}

	// Add our current hash to the end
	if err = h.tf.WriteLine(newHashLine()); err != nil {
		return
	}

	if err = h.tf.Close(); err != nil {
		return
	}

	if err = h.f.Close(); err != nil {
		return
	}

	if err = os.Rename(h.tfLoc, h.fLoc); err != nil {
		return
	}

	if err = h.f.SetFile(); err != nil {
		return
	}

	err = h.f.SeekToEnd()
	return
}

// newReadTx returns a new read transaction, used by read transaction pool
func (h *Hippy) newReadTx() *ReadTx {
	return &ReadTx{h}
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
		h: h,
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
		goto END
	}
	h.closed = true

	if h.opts.ArchiveOnClose {
		if err = h.f.SeekToLastHash(); err != nil {
			goto END
		}

		if err = h.f.Read(h.archive); err != nil {
			goto END
		}
	}

	if h.opts.CompactOnClose {
		err = h.compact()
	}

END:
	h.mux.Unlock()
	return
}
