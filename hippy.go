package hippy

import (
	"bytes"
	"io"
	"os"
	"sync"

	"fmt"
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

	// ErrInvalidTxnType is returned when an invalid transaction type is used
	ErrInvalidTxnType = errors.Error("cannot action using this transaction type")

	// ErrCannotCreateBucket is returned when a bucket cannot be created
	ErrCannotCreateBucket = errors.Error("cannot create bucket")
)

var (
	// Hippy-global buffer pool
	// Note: This might end up in the hippy struct, still deciding what will be the best solution
	bp = bufferPool.New(32)
)

// New returns a new Hippy
func New(opts Opts, mws ...middleware.Middleware) (h *Hippy, err error) {
	// Append Base64 encoding to the end of the middleware chain. This will ensure that we do not have breaking characters within our saved data
	mws = append(mws, middleware.Base64MW{})

	// Create Hippy, he doesn't smell.. quite yet.
	hip := Hippy{
		opts: opts,

		root: &Bucket{m: make(map[string]interface{})},
		mws:  middleware.NewMWs(mws...),
	}

	// Open persistance file
	lfopts := lineFile.Opts{
		Path: opts.Path,
		Name: opts.Name,
		Ext:  "hdb",
	}

	if opts.AsyncBackend {
		lfopts.Backend = lineFile.AsyncBackend
	}

	if hip.f, err = lineFile.New(lfopts); err != nil {
		return
	}

	lfopts.Name = opts.Name + ".archive"
	if hip.af, err = lineFile.New(lfopts); err != nil {
		return
	}

	lfopts.Name = opts.Name + ".tmp"
	lfopts.NoSet = true
	if hip.tf, err = lineFile.New(lfopts); err != nil {
		return
	}

	h = &hip
	// Initialize transaction pools
	h.rtxp = sync.Pool{New: func() interface{} { return h.newreadTxn() }}
	h.utxp = sync.Pool{New: func() interface{} { return h.newupdateTxn() }}

	// Replay file data to populate the database
	err = h.replay()
	return
}

// Hippy is a db
type Hippy struct {
	mux sync.RWMutex

	opts Opts // Options

	root *Bucket         // Root bucket
	mws  *middleware.MWs // Middlewares

	f  *lineFile.File // Persistent storage
	af *lineFile.File // Archive file
	tf *lineFile.File // Temporary file

	rtxp sync.Pool // Read transaction pool
	utxp sync.Pool // Update transaction pool

	closed bool // Closed state
}

// newLogLine will return a new log line given a provided key, action, and body
func (h *Hippy) newLogLine(a byte, keys []string, body []byte) (out *bytes.Buffer, err error) {
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

	// Write key count
	if _, err = mw.Write([]byte{uint8(len(keys))}); err != nil {
		goto ERROR
	}

	for _, key := range keys {
		// Write key length
		if _, err = mw.Write([]byte{uint8(len(key))}); err != nil {
			goto ERROR
		}

		// Write key
		if _, err = mw.Write([]byte(key)); err != nil {
			goto ERROR
		}
	}

	// If the action is not PUT, return
	if a != _put {
		goto END
	}

	// Write body
	if _, err = mw.Write(body); err != nil {
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
func (h *Hippy) parseLogLine(in *bytes.Buffer) (a byte, keys []string, body []byte, err error) {
	var (
		b   []byte
		i   uint8
		kc  uint8 // Key count
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
	kc = uint8(b[i])
	i++

	for j := uint8(0); j < kc; j++ {
		kl = uint8(b[i])
		i++

		keys = append(keys, string(b[i:i+kl]))
		i += kl
	}

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
		a    byte
		keys []string
		val  []byte
		v    interface{}
		bkt  *Bucket
		de   bool // Data exists boolean
	)

	h.mux.Lock()
	h.f.SeekToStart()
	h.f.ReadLines(func(b *bytes.Buffer) (ok bool) {
		if a, keys, val, err = h.parseLogLine(b); err != nil {
			ok = true
			return
		}

		ki := len(keys) - 1
		if bkt, err = createBucket(h.root, keys[:ki], nil, nil); err != nil {
			return
		}

		// Fulfill action
		switch a {
		case _hash:
		case _put:
			if bkt.ufn == nil {
				fmt.Println("Putting raw value", string(val))
				v = RawValue(val)
			} else {
				// Put value by key
				if v, err = bkt.ufn(val); err != nil {
					return
				}
			}

			bkt.m[keys[ki]] = v
		case _del:
			// Delete by key
			delete(bkt.m, keys[ki])
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

	if b, err = h.newLogLine(_hash, []string{hash}, nil); err != nil {
		return
	}

	err = tgt.WriteLine(b.Bytes())
	bp.Put(b)
	return
}

func getBuckets(b *Bucket) (bkts []*Bucket) {
	var (
		bkt *Bucket
		ok  bool
	)

	for _, v := range b.m {
		if bkt, ok = v.(*Bucket); !ok {
			continue
		}

		bkts = append(bkts, getBuckets(bkt)...)
	}

	bkts = append(bkts, b)
	return
}

func getActions(b *Bucket) (acts map[string]action) {
	var (
		a  action
		ok bool
	)

	acts = make(map[string]action, len(b.m))
	for k, v := range b.m {
		if a, ok = v.(action); !ok {
			continue
		}

		acts[k] = a
	}

	return
}

// write will write a transaction to disk
// Note: This is not thread safe. It is expected that the calling function is managing locks
func (h *Hippy) write(actions *Bucket) (err error) {
	var (
		rbkt *Bucket
		body []byte
	)

	for _, bkt := range getBuckets(actions) {
		if bkt.mfn == nil {
			continue
		}

		fmt.Println("Creating bucket!", bkt)
		if rbkt, err = createBucket(h.root, bkt.keys, bkt.mfn, bkt.ufn); err != nil {
			return
		}

		for k, a := range getActions(bkt) {
			if body, err = bkt.mfn(a.v); err != nil {
				return
			}

			if err = h.writeLogLine(a.a, append(bkt.keys, k), body); err != nil {
				return
			}

			// Fulfill action
			switch a.a {
			case _put:
				// Put by key
				rbkt.m[k] = a.v
			case _del:
				// Delete by key
				delete(rbkt.m, k)
				if len(rbkt.m) == 0 {
					if err = h.writeLogLine(_del, bkt.keys, nil); err != nil {
						return
					}
				}
			}
		}

		bktP.Put(bkt)
	}

	return h.f.Flush()
}

func (h *Hippy) writeLogLine(a byte, keys []string, body []byte) (err error) {
	var ll *bytes.Buffer
	if ll, err = h.newLogLine(a, keys, body); err != nil {
		return
	}

	// We are going to write before modifying memory
	if err = h.f.WriteLine(ll.Bytes()); err != nil {
		return
	}

	bp.Put(ll)
	return
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
		li   int // Line index
		keys []string
		pos  = -1
	)

	if err = tgt.SeekToStart(); err != nil {
		return
	}

	tgt.ReadLines(func(b *bytes.Buffer) (ok bool) {
		if bb := b.Bytes(); len(bb) == 0 {
			return
		} else if bb[0] == _hash {
			if _, keys, _, err = h.parseLogLine(b); err != nil {
				ok = true
				return
			}

			if keys[0] == hash || len(hash) == 0 {
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
	var (
		li   int // Line index
		keys []string
	)

	pos = -1
	if err = tgt.SeekToStart(); err != nil {
		return
	}

	tgt.ReadLines(func(b *bytes.Buffer) (ok bool) {
		bb := b.Bytes()
		if bb[0] == _hash {
			pos = li
			if _, keys, _, err = h.parseLogLine(b); err != nil {
				ok = true
				return
			}

			hash = keys[0]
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
		body []byte
	)

	if _, hash, err = h.getLastHash(h.f); err != nil {
		return
	}

	if err = h.tf.Open(); err != nil {
		return
	}

	for _, bkt := range getBuckets(h.root) {
		if bkt.mfn == nil {
			continue
		}

		for k, v := range bkt.m {
			if _, ok := v.(*Bucket); ok {
				continue
			}

			if body, err = bkt.mfn(v); err != nil {
				return
			}

			if ll, err = h.newLogLine(_put, append(bkt.keys, k), body); err != nil {
				return
			}

			// We are going to write before modifying memory
			if err = h.tf.WriteLine(ll.Bytes()); err != nil {
				return
			}

			bp.Put(ll)
			ll = nil
		}

		bktP.Put(bkt)
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

// newreadTxn returns a new read transaction, used by read transaction pool
func (h *Hippy) newreadTxn() *readTxn {
	return &readTxn{h}
}

// newupdateTxn returns a new read/write transaction, used by read/write transaction pool
func (h *Hippy) newupdateTxn() (u *updateTxn) {
	u = &updateTxn{
		h: h,
		a: &Bucket{
			m: make(map[string]interface{}),
		},
	}

	u.a.txn = u
	return
}

// getreadTxn returns a new read transaction from the read transaction pool
func (h *Hippy) getreadTxn() (txn *readTxn) {
	txn, _ = h.rtxp.Get().(*readTxn)
	return
}

// getupdateTxn returns a new read/write transaction from the read/write transaction pool
func (h *Hippy) getupdateTxn() (txn *updateTxn) {
	txn, _ = h.utxp.Get().(*updateTxn)
	return
}

// putreadTxn releases a read transaction back to the read transaction pool
func (h *Hippy) putreadTxn(txn *readTxn) {
	h.rtxp.Put(txn)
}

// putupdateTxn releases a read/write transaction back to the read/write transaction pool
func (h *Hippy) putupdateTxn(txn *updateTxn) {
	for k := range txn.a.m {
		delete(txn.a.m, k)
	}

	h.utxp.Put(txn)
}

// Read will return a read-only transaction
func (h *Hippy) Read(fn TxnFn) (err error) {
	// Get a read transaction from the pool
	txn := h.getreadTxn()

	h.mux.RLock()
	if h.closed {
		err = ErrIsClosed
	} else {
		err = fn(txn)
	}
	h.mux.RUnlock()

	// Return read transaction to the pool
	h.putreadTxn(txn)
	return
}

// Update returns a read/write transaction
func (h *Hippy) Update(fn TxnFn) (err error) {
	// Get a read/write transaction from the pool
	txn := h.getupdateTxn()

	h.mux.Lock()
	if h.closed {
		err = ErrIsClosed
		goto END
	}

	if err = fn(txn); err == nil {
		err = h.write(txn.a)
	}

END:
	h.mux.Unlock()
	// Return read/write transaction to the pool
	h.putupdateTxn(txn)
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
