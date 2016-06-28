package hippy

import (
	"bufio"
	"errors"
	"os"
	"strings"
	"sync"

	"fmt"
)

const (
	_none byte = iota
	_put
	_del

	_separator = ':'
	_newline   = '\n'
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
		s: make(storage),
	}

	if hip.f, err = os.OpenFile(loc, os.O_CREATE|os.O_APPEND|os.O_RDWR, 0644); err != nil {
		return
	}

	h = &hip
	h.rtxp = sync.Pool{
		New: func() interface{} { return h.newReadTx() },
	}

	h.wtxp = sync.Pool{
		New: func() interface{} { return h.newWriteTx() },
	}

	h.rwtxp = sync.Pool{
		New: func() interface{} { return h.newReadWriteTx() },
	}

	h.replay()
	return
}

// Hippy is a db
type Hippy struct {
	mux sync.RWMutex

	s storage
	f *os.File

	// Pools
	rtxp  sync.Pool
	wtxp  sync.Pool
	rwtxp sync.Pool

	closed bool
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
	for scnr.Scan() {
		if a, key, val, err = parseLogLine(scnr.Bytes()); err != nil {
			continue
		}

		switch a {
		case _put:
			h.s[key] = val
		case _del:
			delete(h.s, key)
		}
	}
	h.mux.Unlock()
	fmt.Println(h.s)
}

func (h *Hippy) write(a map[string]action) (err error) {
	for k, v := range a {
		// We are going to write before modifying memory
		if _, err = h.f.Write(newLogLine(k, v.a, v.b)); err != nil {
			return
		}

		switch v.a {
		case _put:
			h.s[k] = v.b
		case _del:
			delete(h.s, k)
		}
	}

	// Call sync to make sure the data has flushed to disk
	return h.f.Sync()
}

func (h *Hippy) newReadTx() *ReadTx {
	return &ReadTx{&h.s}
}

func (h *Hippy) newWriteTx() *WriteTx {
	return &WriteTx{
		a: make(map[string]action),
	}
}

func (h *Hippy) newReadWriteTx() *ReadWriteTx {
	return &ReadWriteTx{
		s: &h.s,
		a: make(map[string]action),
	}
}

func (h *Hippy) getReadTx() (tx *ReadTx) {
	tx, _ = h.rtxp.Get().(*ReadTx)
	return
}

func (h *Hippy) getWriteTx() (tx *WriteTx) {
	tx, _ = h.wtxp.Get().(*WriteTx)
	return
}

func (h *Hippy) getReadWriteTx() (tx *ReadWriteTx) {
	tx, _ = h.rwtxp.Get().(*ReadWriteTx)
	return
}

func (h *Hippy) putReadTx(tx *ReadTx) {
	h.rtxp.Put(tx)
}

func (h *Hippy) putWriteTx(tx *WriteTx) {
	for k := range tx.a {
		delete(tx.a, k)
	}

	h.wtxp.Put(tx)
}

func (h *Hippy) putReadWriteTx(tx *ReadWriteTx) {
	for k := range tx.a {
		delete(tx.a, k)
	}

	h.rwtxp.Put(tx)
}

// Read will return a read-only transaction
func (h *Hippy) Read(fn func(*ReadTx) error) (err error) {
	tx := h.getReadTx()

	h.mux.RLock()
	err = fn(tx)
	h.mux.RUnlock()

	h.putReadTx(tx)
	return
}

// ReadWrite returns a read/write transaction
func (h *Hippy) ReadWrite(fn func(*ReadWriteTx) error) (err error) {
	tx := h.getReadWriteTx()

	h.mux.Lock()
	if err = fn(tx); err == nil {
		err = h.write(tx.a)
	}
	h.mux.Unlock()

	h.putReadWriteTx(tx)
	return
}

// Write returns a write-only transaction
func (h *Hippy) Write(fn func(*WriteTx) error) (err error) {
	tx := h.getWriteTx()

	h.mux.Lock()
	if err = fn(tx); err == nil {
		err = h.write(tx.a)
	}
	h.mux.Unlock()

	h.putWriteTx(tx)
	return
}

// ReadTx is a read-only transaction
type ReadTx struct {
	s *storage
}

// Get will get
func (r *ReadTx) Get(k string) (b []byte, ok bool) {
	var tgt []byte
	s := *r.s
	if tgt, ok = s[k]; !ok {
		return
	}

	b = make([]byte, len(tgt))
	copy(b, tgt)
	return
}

// Keys will list the keys for a DB
func (r *ReadTx) Keys() (keys []string) {
	keys = make([]string, 0, len(*r.s))
	for k := range *r.s {
		keys = append(keys, k)
	}

	return
}

// ReadWriteTx is a read/write transaction
type ReadWriteTx struct {
	mux sync.RWMutex

	s *storage
	a map[string]action
}

// Get will get
func (rw *ReadWriteTx) Get(k string) (b []byte, ok bool) {
	var (
		ta  action
		tgt []byte
		s   storage
	)

	rw.mux.RLock()
	if ta, ok = rw.a[k]; ok {
		if ta.a == _put {
			tgt = ta.b
			goto COPY
		}

		ok = false
		goto END
	}

	s = *rw.s
	if tgt, ok = s[k]; !ok {
		goto END
	}

COPY:
	b = make([]byte, len(tgt))
	copy(b, tgt)

END:
	rw.mux.RUnlock()
	return

}

// Put will put
func (rw *ReadWriteTx) Put(k string, v []byte) (err error) {
	if strings.IndexByte(k, _separator) > -1 {
		return ErrInvalidKey
	}

	rw.mux.Lock()
	rw.a[k] = action{
		a: _put,
		b: v,
	}
	rw.mux.Unlock()
	return
}

// Del will delete
func (rw *ReadWriteTx) Del(k string) {
	rw.mux.Lock()
	rw.a[k] = action{
		a: _del,
	}
	rw.mux.Unlock()
}

// Keys will list the keys for a DB
func (rw *ReadWriteTx) Keys() (keys []string) {
	keys = make([]string, 0, len(*rw.s))
	for k := range *rw.s {
		keys = append(keys, k)
	}

	return
}

// WriteTx is a write-only transaction
type WriteTx struct {
	mux sync.Mutex

	a map[string]action
}

// Put will put
func (w *WriteTx) Put(k string, v []byte) (err error) {
	if strings.IndexByte(k, _separator) > -1 {
		return ErrInvalidKey
	}

	w.mux.Lock()
	w.a[k] = action{
		a: _put,
		b: v,
	}
	w.mux.Unlock()
	return
}

// Del will delete
func (w *WriteTx) Del(k string) {
	w.mux.Lock()
	w.a[k] = action{
		a: _del,
	}
	w.mux.Unlock()
}

type action struct {
	a byte
	b []byte
}

type storage map[string][]byte

func newLogLine(key string, a byte, b []byte) (out []byte) {
	out = append(out, byte(a))
	out = append(out, key...)
	if a == _del {
		goto END
	}

	out = append(out, _separator)
	out = append(out, b...)

END:
	out = append(out, _newline)
	return
}

func parseLogLine(b []byte) (a byte, key string, val []byte, err error) {
	a = b[0]
	switch a {
	case _put, _del:
	default:
		err = ErrInvalidAction
		return
	}

	var (
		keyB []byte
		i    = 1
	)

	for ; i < len(b); i++ {
		if b[i] == _separator {
			break
		}

		keyB = append(keyB, b[i])
	}

	if a == _del {
		return
	}

	i++
	key = string(keyB)
	val = make([]byte, len(b)-i)
	copy(val, b[i:])
	return
}
