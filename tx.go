package hippy

import "sync"

// ReadTx is a read-only transaction
type ReadTx struct {
	// Pointer to our DB's internal store
	h *Hippy
}

// Get will get a body and an ok value
func (r *ReadTx) Get(k string) (b []byte, ok bool) {
	var tgt []byte
	// Get a non-pointer reference to storage
	if tgt, ok = r.h.s[k]; !ok {
		// Target does not exist, return
		return
	}

	if !r.h.opts.CopyOnRead {
		b = tgt
		return
	}

	// Pre-allocate b to be the length of target
	b = make([]byte, len(tgt))
	// Copy target to b
	copy(b, tgt)
	return
}

// Keys will list the keys for a DB
func (r *ReadTx) Keys() (keys []string) {
	// Pre-allocate keys to be the length of our internal storage
	keys = make([]string, 0, len(r.h.s))

	// For each item in our internal storage, append key to keys
	for k := range r.h.s {
		keys = append(keys, k)
	}

	return
}

// ReadWriteTx is a read/write transaction
type ReadWriteTx struct {
	mux sync.RWMutex

	// Pointer to our DB's internal store
	h *Hippy
	// Actions map
	a map[string]action
}

// Get will get a body and an ok value
func (rw *ReadWriteTx) Get(k string) (b []byte, ok bool) {
	var (
		ta  action
		tgt []byte
	)

	rw.mux.RLock()
	// If action exists for this key..
	if ta, ok = rw.a[k]; ok {
		// If action is PUT, set our target to the action body and goto copy
		if ta.a == _put {
			tgt = ta.b
			goto COPY
		}

		// Action was DELETE, set ok to false and goto end
		ok = false
		goto END
	}

	// Get a non-pointer reference to storage
	if tgt, ok = rw.h.s[k]; !ok {
		// Target does not exist, goto end
		goto END
	}

COPY:
	if !rw.h.opts.CopyOnRead {
		b = tgt
		goto END
	}

	// Pre-allocate b to be the length of target
	b = make([]byte, len(tgt))
	// Copy target to b
	copy(b, tgt)

END:
	rw.mux.RUnlock()
	return

}

// Put will put
func (rw *ReadWriteTx) Put(k string, v []byte) (err error) {
	if len(k) > MaxKeyLen {
		return ErrInvalidKey
	}

	// Create action
	act := action{a: _put}
	rw.mux.Lock()
	if !rw.h.opts.CopyOnWrite {
		// Set action body to value and goto the end
		act.b = v
		goto END
	}

	// Pre-allocate action body to be the length of value
	act.b = make([]byte, len(v))
	// Copy value to action body
	copy(act.b, v)

END:
	rw.a[k] = act
	rw.mux.Unlock()
	return
}

// Del will delete
func (rw *ReadWriteTx) Del(k string) {
	rw.mux.Lock()
	// Set a delete action
	rw.a[k] = action{
		a: _del,
	}
	rw.mux.Unlock()
}

// Keys will list the keys for a DB
func (rw *ReadWriteTx) Keys() (keys []string) {
	// Pre-allocate keys to be the length of our internal storage
	keys = make([]string, 0, len(rw.h.s))
	// For each item in our internal storage, append key to keys
	for k := range rw.h.s {
		keys = append(keys, k)
	}

	return
}

// WriteTx is a write-only transaction
type WriteTx struct {
	mux sync.Mutex

	// Actions map
	a map[string]action
}

// Put will put
func (w *WriteTx) Put(k string, v []byte) (err error) {
	if len(k) > MaxKeyLen {
		return ErrInvalidKey
	}

	w.mux.Lock()
	// Set a put action with the body
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
	// Set a delete action
	w.a[k] = action{
		a: _del,
	}
	w.mux.Unlock()
}
