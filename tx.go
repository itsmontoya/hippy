package hippy

import "sync"
import "fmt"

// RTx is a read transaction interface
type RTx interface {
	Get(k string) (v interface{}, ok bool)
	Keys() (ks []string)
}

// WTx is a write transaction interface
type WTx interface {
	Put(k string, v interface{}) (err error)
	Del(k string)
	Keys() (ks []string)
}

// RWTx is a read-write transaction interface
type RWTx interface {
	Get(k string) (v interface{}, ok bool)
	Put(k string, v interface{}) (err error)
	Del(k string)
	Keys() (ks []string)
}

// ReadTx is a read-only transaction
type ReadTx struct {
	// Pointer to our DB's internal store
	h *Hippy
}

// Get will get a body and an ok value
func (r *ReadTx) get(keys []string) (v interface{}) {
	var (
		bkt *Bucket
		ok  bool

		ki = len(keys) - 1 // Key index
		k  = keys[ki]      // Key
	)

	// We are going to retreive the bucket with matching keys
	// Note: We omit the last key, as this is for the value - rather than the buckets!
	if bkt = r.h.root.bucket(keys[:ki]); bkt == nil {
		// Nothing was found, return!
		return
	}

	// Try to get value at bucket matching our key
	if v, ok = bkt.m[k]; !ok {
		// Value was not found for this key, set v to nil
		v = nil
	}

	return

}

// Keys will list the keys for a DB
func (r *ReadTx) keys(keys []string) (out []string) {
	var (
		bkt *Bucket
		om  = make(map[string]struct{}) // Out map, used for tracking keys
	)

	// Get bucket matching provided keys from root
	if bkt = r.h.root.bucket(keys); bkt == nil {
		// No match exists, jump to the transaction lookup
		return
	}

	// Iterate through bucket's map keys, set out map with each iteration
	for k := range bkt.m {
		om[k] = struct{}{}
	}

	// Make our outbound slice with a capacity as the length of our out map
	out = make([]string, 0, len(om))

	for k := range om {
		out = append(out, k)
	}

	return
}

// Bucket will retrieve a bucket
func (r *ReadTx) Bucket(keys ...string) (bkt *Bucket) {
	return r.h.root.bucket(keys)
}

// ReadWriteTx is a read/write transaction
type ReadWriteTx struct {
	mux sync.RWMutex

	// Pointer to our DB's internal store
	h *Hippy

	// Actions
	a *Bucket
}

// Get will get a body and an ok value
func (rw *ReadWriteTx) get(keys []string) (v interface{}) {
	var (
		ta  action
		bkt *Bucket
		ok  bool

		ki = len(keys) - 1 // Key index
		k  = keys[ki]      // Key
	)

	fmt.Println("Getting!", keys)

	// Now that our variable are allocated, let's lock!
	rw.mux.RLock()
	// We are going to retreive the bucket with matching keys
	// Note: We omit the last key, as this is for the value - rather than the buckets!
	if bkt = rw.a.bucket(keys[:ki]); bkt == nil {
		// No match, goto root
		goto ROOT
	}

	// Try to get value at bucket matching our key
	if v, ok = bkt.m[k]; ok {
		// Attempt to assert type as action
		if ta, ok = v.(action); !ok {
			// This is a bucket, set v to nil and move along
			v = nil
			goto END
		}

		// Set the value to our transaction-level value
		if ta.a == _put {
			// Will set as newest value
			v = ta.v
		} else {
			// Value was deleted during the transaction, v is nil
			v = nil
		}

		// We found what we were looking for, goto the end
		goto END
	}

ROOT:
	// Set bkt to a bucket lookup from root
	if bkt = rw.h.root.bucket(keys[:ki]); bkt == nil {
		// Nothing was found, goto the end
		goto END
	}

	if v, ok = bkt.m[k]; !ok {
		// Value was not found for this key, set v to nil
		v = nil
	}

END:
	rw.mux.RUnlock()
	return

}

// Put will put
func (rw *ReadWriteTx) put(keys []string, v interface{}) (err error) {
	var (
		bkt *Bucket

		ki  = len(keys) - 1
		k   = keys[ki]
		act = action{a: _put, v: v} // Create action
	)

	if len(k) > MaxKeyLen {
		err = ErrInvalidKey
		return
	}

	fmt.Println("About to put", keys)
	rw.mux.Lock()
	if bkt, err = rw.a.createBucket(keys[:ki]); err != nil {
		goto END
	}

	bkt.m[k] = act
END:
	rw.mux.Unlock()
	return
}

// Del will delete
func (rw *ReadWriteTx) del(keys []string) {
	var (
		bkt *Bucket

		ki  = len(keys) - 1
		k   = keys[ki]
		act = action{a: _del} // Create action
	)

	if len(k) > MaxKeyLen {
		return
	}

	rw.mux.Lock()
	if bkt = rw.a.bucket(keys[:ki]); bkt == nil {
		goto END
	}

	bkt.m[k] = act

END:
	rw.mux.Unlock()
	return
}

// Keys will list the keys for a DB
func (rw *ReadWriteTx) keys(keys []string) (out []string) {
	var (
		bkt *Bucket
		a   action
		ok  bool

		om = make(map[string]struct{}) // Out map, used for tracking keys
	)

	rw.mux.RLock()
	// Get bucket matching provided keys from root
	if bkt = rw.h.root.bucket(keys); bkt == nil {
		// No match exists, jump to the transaction lookup
		goto TXN
	}

	// Iterate through bucket's map keys, set out map with each iteration
	for k := range bkt.m {
		om[k] = struct{}{}
	}

TXN:
	// Get bucket matching provided keys from the transaction
	if bkt = rw.a.bucket(keys); bkt != nil {
		// No match exists, jump to the end
		goto END
	}

	// Iterate through bucket's map keys, for each iteration:
	// 	- Assert value as an action
	// 		- If value is not an action, continue
	// 	- If action is a put, set out map
	// 	- If action is a del, remove key from out map
	for k, v := range bkt.m {
		if a, ok = v.(action); !ok {
			continue
		}

		switch a.a {
		case _put:
			om[k] = struct{}{}

		case _del:
			delete(om, k)
		}
	}

END:
	rw.mux.RUnlock()
	// Make our outbound slice with a capacity as the length of our out map
	out = make([]string, 0, len(om))

	for k := range om {
		out = append(out, k)
	}

	return
}

// CreateBucket will create a bucket
func (rw *ReadWriteTx) CreateBucket(key string, mfn MarshalFn, ufn UnmarshalFn) (err error) {
	fmt.Println("Creating bucket", key)
	return rw.a.CreateBucket(key, mfn, ufn)
}

// Bucket will retrieve a bucket
func (rw *ReadWriteTx) Bucket(keys ...string) (bkt *Bucket) {
	if bkt = rw.a.bucket(keys); bkt != nil {
		return
	}

	if bkt = rw.h.root.bucket(keys); bkt != nil {
		bkt, _ = rw.a.createBucket(keys)
		bkt.txn = rw
	}

	return
}

// WriteTx is a read/write transaction
type WriteTx struct {
	mux sync.RWMutex

	// Actions
	a *Bucket
}
