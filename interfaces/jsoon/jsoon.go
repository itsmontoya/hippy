package hippyJsoon

import (
	"bytes"

	"github.com/itsmontoya/hippy"
	"github.com/itsmontoya/jsoon"
	"github.com/itsmontoya/middleware"
	"github.com/missionMeteora/toolkit/errors"
)

const (
	// ErrNoValueAtKey is returned when no value exists at a requested key
	ErrNoValueAtKey = errors.Error("no value exists at requested key")
)

// New returns a new instance of Hippy (jsoon)
func New(path, name string, opts hippy.Opts, mws ...middleware.Middleware) (h *Hippy, err error) {
	var hp Hippy
	if hp.h, err = hippy.New(path, name, opts, mws...); err != nil {
		return
	}

	h = &hp
	return
}

// Hippy is an interface for a jsoon-based hippy.Hippy
type Hippy struct {
	h *hippy.Hippy
}

// Read is for read-only operations
func (h *Hippy) Read(fn func(*ReadTx) error) (err error) {
	return h.h.Read(func(tx *hippy.ReadTx) (err error) {
		rtx := ReadTx{tx: tx}
		err = fn(&rtx)
		rtx.tx = nil
		return
	})
}

// Write is for write-only operations
func (h *Hippy) Write(fn func(*WriteTx) error) (err error) {
	return h.h.Write(func(tx *hippy.WriteTx) (err error) {
		wtx := WriteTx{tx: tx}
		err = fn(&wtx)
		wtx.tx = nil
		return
	})
}

// ReadWrite is for read/write operations
func (h *Hippy) ReadWrite(fn func(*ReadWriteTx) error) (err error) {
	return h.h.ReadWrite(func(tx *hippy.ReadWriteTx) (err error) {
		rwtx := ReadWriteTx{tx: tx}
		err = fn(&rwtx)
		rwtx.tx = nil
		return
	})
}

// ReadTx is for read-only operations
type ReadTx struct {
	tx *hippy.ReadTx
}

// Get will get
func (r *ReadTx) Get(key string, val interface{}) (err error) {
	var (
		b  []byte
		ok bool
	)

	if b, ok = r.tx.Get(key); !ok {
		return ErrNoValueAtKey
	}

	return jsoon.NewDecoder(bytes.NewReader(b)).Decode(val)
}

// Keys will list keys
func (r *ReadTx) Keys() []string {
	return r.tx.Keys()
}

// ReadWriteTx is for read/write operations
type ReadWriteTx struct {
	tx *hippy.ReadWriteTx
}

// Get will get
func (rw *ReadWriteTx) Get(key string, val interface{}) (err error) {
	var (
		b  []byte
		ok bool
	)

	if b, ok = rw.tx.Get(key); !ok {
		return ErrNoValueAtKey
	}

	return jsoon.NewDecoder(bytes.NewReader(b)).Decode(val)
}

// Put will put
func (rw *ReadWriteTx) Put(key string, val jsoon.Encodee) (err error) {
	buf := bytes.NewBuffer(nil)
	if err = jsoon.NewEncoder(buf).Encode(val); err != nil {
		return
	}

	return rw.tx.Put(key, buf.Bytes())
}

// Del will delete
func (rw *ReadWriteTx) Del(key string) {
	rw.tx.Del(key)
}

// Keys will list keys
func (rw *ReadWriteTx) Keys() []string {
	return rw.tx.Keys()
}

// WriteTx is for write-only operations
type WriteTx struct {
	tx *hippy.WriteTx
}

// Put will put
func (w *WriteTx) Put(key string, val jsoon.Encodee) (err error) {
	buf := bytes.NewBuffer(nil)
	if err = jsoon.NewEncoder(buf).Encode(val); err != nil {
		return
	}

	return w.tx.Put(key, buf.Bytes())
}

// Del will delete
func (w *WriteTx) Del(key string) {
	w.tx.Del(key)
}
