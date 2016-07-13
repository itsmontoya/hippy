package hippy

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/itsmontoya/lineFile"
	"github.com/itsmontoya/middleware"
	"github.com/missionMeteora/toolkit/bufferPool"

	"fmt"
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

	// ErrLineNotFound is returned when a line is not found while calling SeekNextLine
	ErrLineNotFound = Error("line not found")

	// ErrIsClosed is returned when an action is attempted on a closed instance
	ErrIsClosed = Error("cannot perform action on closed instance")

	// ErrIsOpen is returned when an instance is attempted to be re-opened when it's already active
	ErrIsOpen = Error("cannot open an instance which is already open")
)

var (
	// Hippy-global buffer pool
	// Note: This might end up in the hippy struct, still deciding what will be the best solution
	bp = bufferPool.New(32)
)

// New returns a new Hippy
func New(path, name string, opts Opts, mws ...Middleware) (h *Hippy, err error) {
	mws = append(mws, b64MW{})
	hip := Hippy{
		// Make the internal storage map, it would be a shame to panic on put!
		s: make(storage),

		fLoc:  filepath.Join(path, name+".hdb"),
		tfLoc: filepath.Join(path, name+".tmp.hdb"),

		path: path,
		name: name,
		mws:  middleware.NewMWs(mws...),
		opts: opts,
	}

	// Open persistance file
	lfopts := lineFile.Opts{
		Path: path,
		Name: name,
		Ext:  "hdb",
	}

	if hip.f, err = lineFile.New(lfopts); err != nil {
		return
	}

	lfopts.Name = name + ".archive"
	if hip.af, err = lineFile.New(lfopts); err != nil {
		return
	}

	lfopts.Name = name + ".tmp"
	lfopts.NoSet = true
	if hip.tf, err = lineFile.New(lfopts); err != nil {
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

	path string // Database path
	name string // Database name
	opts Opts   // Options

	s   storage         // In-memory storage
	mws *middleware.MWs // Middlewares

	f  *lineFile.File // Persistent storage
	af *lineFile.File // Archive file
	tf *lineFile.File // Temporary file

	rtxp  sync.Pool // Read transaction pool
	wtxp  sync.Pool // Write transaction pool
	rwtxp sync.Pool // Read/Write transaction pool

	closed bool // Closed state
}

func (h *Hippy) replay(fr *fileReader) (err error) {
	var de bool // Data exists boolean

	h.mux.Lock()
	// For each line..
	fr.ReadLines(func(a byte, key string, val []byte, err error) error {
		if err != nil {
			return err
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
		default:
			return nil
		}

		// We know data exists, let's set de to true
		de = true
		return nil
	})

	if !de {
		err = h.f.WriteLine(newHashLine())
	}

	h.mux.Unlock()
	return
}

// write will write a transaction to disk
// Note: This is not thread safe. It is expected that the calling function is managing locks
func (h *Hippy) write(a map[string]action) (err error) {
	var ll *bytes.Buffer
	for k, v := range a {
		ll = newLogLine(v.a, k, v.b)

		// We are going to write before modifying memory
		if err = h.f.WriteLine(ll.Bytes()); err != nil {
			return
		}

		bp.Put(ll)
		ll = nil

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

func (h *Hippy) archive() (err error) {
	var n int64 // Written count

	if err = h.f.Flush(); err != nil {
		return
	}

	if err = h.f.SeekToLastHash(); err != nil {
		return
	}

	if err = h.f.SeekToNextLine(); err != nil {
		fmt.Println("Cannot seek to next line!", err)
		return
	}

	if err = h.f.SeekToPrevLine(); err != nil {
		fmt.Println("Cannot seek to prev line!", err)
		return
	}

	if err = h.af.SeekToEnd(); err != nil {
		return
	}

	if n, err = io.Copy(h.af.f, h.f.f); err != nil || n == 0 {
		return
	}

	if err = h.f.WriteLine(newHashLine()); err != nil {
		return
	}

	if err = h.f.Flush(); err != nil {
		return
	}

	return h.af.Flush()
}

func (h *Hippy) compact() (err error) {
	var ll *bytes.Buffer
	if err = h.tf.SetFile(); err != nil {
		return
	}

	// Write data contents to tmp file
	for k, v := range h.s {
		ll = newLogLine(_put, k, v)
		if err = h.tf.WriteLine(ll.Bytes()); err != nil {
			return
		}

		bp.Put(ll)
		ll = nil
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

// Close will close Hippy
func (h *Hippy) Close() (err error) {
	h.mux.Lock()
	if h.closed {
		err = ErrIsClosed
		goto END
	}
	h.closed = true

	if h.opts.ArchiveOnClose {
		if err = h.archive(); err != nil {
			goto END
		}
	}

	if h.opts.CompactOnClose {
		//	err = h.compact()
	}

END:
	fmt.Println("Close complete!", err)
	h.mux.Unlock()
	return
}
