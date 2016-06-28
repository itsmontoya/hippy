package hippy

import (
	"strings"
	"sync"
)

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
