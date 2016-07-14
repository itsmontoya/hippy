package hippy

import (
	"bytes"
	"io"
	"os"
	"sync"

	"github.com/itsmontoya/lineFile"
	"github.com/itsmontoya/middleware"
	"github.com/missionMeteora/toolkit/bufferPool"
	"github.com/missionMeteora/toolkit/errors"
	"github.com/missionMeteora/uuid"
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
	ErrInvalidAction = errors.Error("invalid action")

	// ErrInvalidKey is returned when an invalid key is provided
	ErrInvalidKey = errors.Error("invalid key")

	// ErrHashLine is returned when a hash line is parsed as a log line
	ErrHashLine = errors.Error("cannot parse hash line as log line")

	// ErrHashNotFound is returned when a hash line is not found while archiving
	ErrHashNotFound = errors.Error("hash not found")

	// ErrLineNotFound is returned when a line is not found while calling SeekNextLine
	ErrLineNotFound = errors.Error("line not found")

	// ErrIsClosed is returned when an action is attempted on a closed instance
	ErrIsClosed = errors.Error("cannot perform action on closed instance")

	// ErrIsOpen is returned when an instance is attempted to be re-opened when it's already active
	ErrIsOpen = errors.Error("cannot open an instance which is already open")

	// ErrNoChanges is returned when no changes occur and an archive is not needed
	ErrNoChanges = errors.Error("no changes occured, archive not necessary")
)

var (
	// Hippy-global buffer pool
	// Note: This might end up in the hippy struct, still deciding what will be the best solution
	bp = bufferPool.New(32)
)

// New returns a new Hippy
func New(path, name string, opts Opts, mws ...middleware.Middleware) (h *Hippy, err error) {
	// Append Base64 encoding to the end of the middleware chain. This will ensure that we do not have breaking characters within our saved data
	mws = append(mws, middleware.Base64MW{})

	// Create Hippy, he doesn't smell.. quite yet.
	hip := Hippy{
		// Make the internal storage map, it would be a shame to panic on put!
		s:    make(storage),
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
	err = h.replay()
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

// newLogLine will return a new log line given a provided key, action, and body
func (h *Hippy) newLogLine(a byte, key string, b []byte) (out *bytes.Buffer, err error) {
	var mw *middleware.Writer
	// Get buffer from the buffer pool
	out = bp.Get()

	// Write action
	if err = out.WriteByte(byte(a)); err != nil {
		goto ERROR
	}

	// Get middleware writer
	if mw, err = h.mws.Writer(out); err != nil {
		goto ERROR
	}

	// Write key length
	if _, err = mw.Write([]byte{uint8(len(key))}); err != nil {
		goto ERROR
	}

	// Write key
	if _, err = mw.Write([]byte(key)); err != nil {
		goto ERROR
	}

	// If the action is not PUT, return
	if a != _put {
		goto END
	}

	// Write body
	if _, err = mw.Write(b); err != nil {
		goto ERROR
	}

END:
	mw.Close()
	return

ERROR:
	if mw != nil {
		mw.Close()
	}

	bp.Put(out)
	bp = nil
	return
}

// parseLogLine will return an action, key, and body from a provided log line (in the form of a byte slice)
func (h *Hippy) parseLogLine(in *bytes.Buffer) (a byte, key string, body []byte, err error) {
	var (
		b   []byte
		i   uint8
		kl  uint8 // Key length
		rdr *middleware.Reader
	)

	if a, err = in.ReadByte(); err != nil {
		return
	}

	// Validate action
	switch a {
	case _put, _del, _hash:
	default:
		// Invalid action, return ErrInvalidAction
		err = ErrInvalidAction
		return
	}

	if rdr, err = h.mws.Reader(in); err != nil {
		return
	}

	buf := bp.Get()
	if _, err = io.Copy(buf, rdr); err != nil {
		goto END
	}

	b = buf.Bytes()
	kl = uint8(b[i])
	i++

	key = string(b[i : i+kl])
	i += kl

	// If our action is not PUT, we do not need to parse any further
	if a != _put {
		goto END
	}

	b = b[i:]
	body = make([]byte, len(b))
	copy(body, b)

END:
	rdr.Close()
	bp.Put(buf)
	return
}

func (h *Hippy) replay() (err error) {
	var (
		a   byte
		key string
		val []byte
		de  bool // Data exists boolean
	)

	h.mux.Lock()
	h.f.SeekToStart()
	h.f.ReadLines(func(b *bytes.Buffer) (ok bool) {
		if a, key, val, err = h.parseLogLine(b); err != nil {
			return ok
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
			return
		}

		// We know data exists, let's set de to true
		de = true
		return
	})

	if err == nil && !de {
		h.newHashLine(h.f, "")
	}

	h.mux.Unlock()
	return
}

func (h *Hippy) newHashLine(tgt *lineFile.File, hash string) (err error) {
	var b *bytes.Buffer
	if len(hash) == 0 {
		hash = uuid.New().String()
	}

	if b, err = h.newLogLine(_hash, hash, nil); err != nil {
		return
	}

	err = tgt.WriteLine(b.Bytes())
	bp.Put(b)
	return
}

// write will write a transaction to disk
// Note: This is not thread safe. It is expected that the calling function is managing locks
func (h *Hippy) write(a map[string]action) (err error) {
	var ll *bytes.Buffer
	for k, v := range a {
		if ll, err = h.newLogLine(v.a, k, v.b); err != nil {
			return
		}

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

func (h *Hippy) seekToLastHash(tgt *lineFile.File) (err error) {
	var pos int // Hash position
	if pos, _, err = h.getLastHash(tgt); err != nil {
		return
	}

	err = tgt.SeekToLine(pos)
	return
}

func (h *Hippy) seekToHash(tgt *lineFile.File, hash string) (err error) {
	var (
		li  int // Line index
		key string
		pos = -1
	)

	if err = tgt.SeekToStart(); err != nil {
		return
	}

	tgt.ReadLines(func(b *bytes.Buffer) (ok bool) {
		if bb := b.Bytes(); len(bb) == 0 {
			return
		} else if bb[0] == _hash {
			if _, key, _, err = h.parseLogLine(b); err != nil {
				ok = true
				return
			}

			if key == hash || len(hash) == 0 {
				pos = li
				ok = true
			}
		}

		li++
		return
	})

	if err != nil {
		return
	}

	if pos == -1 {
		err = ErrHashNotFound
	} else {
		err = tgt.SeekToLine(pos)
	}
	return
}

func (h *Hippy) getLastHash(tgt *lineFile.File) (pos int, hash string, err error) {
	var li int // Line index
	pos = -1
	if err = tgt.SeekToStart(); err != nil {
		return
	}

	tgt.ReadLines(func(b *bytes.Buffer) (ok bool) {
		bb := b.Bytes()
		if bb[0] == _hash {
			pos = li
			if _, hash, _, err = h.parseLogLine(b); err != nil {
				ok = true
				return
			}
		}

		li++
		return
	})

	if err == nil && pos == -1 {
		err = ErrHashNotFound
	}
	return
}

func (h *Hippy) getArchivePoint() (hash string, err error) {
	if _, hash, err = h.getLastHash(h.af); err != nil && err != ErrHashNotFound {
		return
	}

	if err = h.seekToHash(h.f, hash); err != nil {
		return
	}

	if err = h.f.NextLine(); err != nil {
		err = ErrNoChanges
	}

	return
}

func (h *Hippy) archive() (err error) {
	if err = h.f.Flush(); err != nil {
		return
	}

	var hash string
	if hash, err = h.getArchivePoint(); err != nil {
		return
	}

	if err = h.f.SeekToEnd(); err != nil {
		return
	}

	if err = h.newHashLine(h.f, ""); err != nil {
		return
	}

	if err = h.f.Flush(); err != nil {
		return
	}

	if err = h.seekToHash(h.f, hash); err != nil {
		return
	}

	if err = h.f.NextLine(); err != nil {
		return
	}

	if err = h.af.SeekToEnd(); err != nil {
		return
	}

	if err = h.af.Append(h.f); err != nil {
		return
	}

	return h.af.Flush()
}

func (h *Hippy) compact() (err error) {
	var (
		hash string
		ll   *bytes.Buffer
	)

	if _, hash, err = h.getLastHash(h.f); err != nil {
		return
	}

	if err = h.tf.Open(); err != nil {
		return
	}

	// Write data contents to tmp file
	for k, v := range h.s {
		if ll, err = h.newLogLine(_put, k, v); err != nil {
			goto ITEREND
		}

		err = h.tf.WriteLine(ll.Bytes())

	ITEREND:
		bp.Put(ll)
		ll = nil

		if err != nil {
			return
		}
	}

	// Add our current hash to the end
	if err = h.newHashLine(h.tf, hash); err != nil {
		return
	}

	if err = h.tf.Close(); err != nil {
		return
	}

	if err = h.f.Close(); err != nil {
		return
	}

	if err = os.Rename(h.tf.Location(), h.f.Location()); err != nil {
		return
	}

	if err = h.f.Open(); err != nil {
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
		if err = h.archive(); err == ErrNoChanges {
			err = nil
		} else if err != nil {
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
