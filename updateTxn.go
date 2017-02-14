package hippy

import "sync"

// updateTxn is a read/write transaction
type updateTxn struct {
	mux sync.RWMutex

	// Pointer to our DB's internal store
	h *Hippy
	// Copy on read
	cor bool
	// Copy on write
	cow bool

	// Actions
	a *Bucket
}

// Bucket will retrieve a bucket
func (txn *updateTxn) Bucket(keys ...string) (bkt *Bucket) {
	return txn.bucket(keys)
}

// CreateBucket will create a bucket
func (txn *updateTxn) CreateBucket(key string, mfn MarshalFn, ufn UnmarshalFn) (*Bucket, error) {
	return txn.a.CreateBucket(key, mfn, ufn)
}

// DeleteBucket will delete a bucket
func (txn *updateTxn) DeleteBucket(keys ...string) error {
	return txn.deleteBucket(keys)
}

// Buckets will retrieve a list of buckets
func (txn *updateTxn) Buckets() (bs []string) {
	return txn.buckets(nil)
}

func (txn *updateTxn) bucket(keys []string) (bkt *Bucket) {
	if bkt = txn.a.bucket(keys); bkt != nil {
		// Bucket exists within our actions, return early
		return
	}

	if bkt = txn.h.root.bucket(keys); bkt == nil {
		// Bucket does not exist within our actions NOR within our root bucket, return early
		return
	}

	// Bucket exists within our root bucket, so we need to make a new bucket within our actions
	if nb, err := txn.createBucket(keys, nil, nil); err == nil {
		nb.mfn = bkt.mfn
		nb.ufn = bkt.ufn
		bkt = nb
	}

	return
}

func (txn *updateTxn) createBucket(keys []string, mfn MarshalFn, ufn UnmarshalFn) (bkt *Bucket, err error) {
	return createBucket(txn.a, keys, mfn, ufn)
}

func (txn *updateTxn) deleteBucket(keys []string) (err error) {
	var bkt, pbkt *Bucket
	// Last key index
	lki := len(keys) - 1
	key := keys[lki]

	// Attempt to get target bucket
	if bkt = txn.bucket(keys); bkt == nil {
		// This bucket doesn't exist, no use deleting it
		return
	}

	// Get the parent bucket
	if pbkt = txn.bucket(keys[:lki]); pbkt == nil {
		// Parent bucket doesn't exist, no use in deleting it's children
		return
	}

	txn.forEach(keys, func(k string, v interface{}) (err error) {
		switch val := v.(type) {
		case *Bucket:
			err = txn.deleteBucket(val.keys)
		case Duper, action:
			pbkt.m[key] = action{a: _del}
		}

		return
	})

	return
}

func (txn *updateTxn) buckets(keys []string) (bs []string) {
	bm := make(map[string]struct{})

	if rbkt := txn.h.root.bucket(keys); rbkt != nil {
		for _, k := range rbkt.buckets() {
			bm[k] = struct{}{}
		}
	}

	if abkt := txn.bucket(keys); abkt != nil {
		for k, v := range abkt.m {
			switch val := v.(type) {
			case action:
				if val.a == _del {
					delete(bm, k)
				}

			case *Bucket:
				bm[k] = struct{}{}
			}
		}
	}

	bs = make([]string, 0, len(bm))

	for key := range bm {
		bs = append(bs, key)
	}

	return
}

// Get will get a value at a given key
func (txn *updateTxn) get(keys []string, k string) (d Duper) {
	var (
		ta  action
		bkt *Bucket
		ok  bool
	)

	// Now that our variable are allocated, let's lock!
	txn.mux.RLock()
	// We are going to retreive the bucket with matching keys
	if bkt = txn.a.bucket(keys); bkt == nil {
		// No match, goto root
		goto ROOT
	}

	// Try to get value at bucket matching our key and attempt to assert type as action
	switch v := bkt.m[k].(type) {
	case action:
		ta = v
	case nil:
		goto ROOT
	}

	// Set the value to our transaction-level value
	if ta.a == _put {
		// Will set as newest value
		if txn.h.opts.CopyOnWrite {
			d = ta.v.Dup()
		} else {
			d = ta.v
		}
	} else {
		// Value was deleted during the transaction, v is nil
		d = nil
	}

	// We found what we were looking for, goto the end
	goto END

ROOT:
	// Set bkt to a bucket lookup from root
	if bkt = txn.h.root.bucket(keys); bkt == nil {
		// Nothing was found, goto the end
		goto END
	}

	if d, ok = bkt.m[k].(Duper); !ok {
		goto END
	}

	if txn.h.opts.CopyOnRead {
		d = d.Dup()
	}

END:
	txn.mux.RUnlock()
	return
}

// has will return whether or not a key exists
func (txn *updateTxn) has(keys []string, k string) bool {
	var (
		bkt  *Bucket
		abkt *Bucket
		a    action
		ok   bool
	)

	// We are going to retreive the bucket with matching keys
	if bkt = txn.h.root.bucket(keys); bkt == nil {
		// Nothing was found, return!
		return false
	}

	_, ok = bkt.m[k]

	// We are going to retreive the bucket with matching keys
	if abkt = txn.h.root.bucket(keys); abkt == nil {
		// No actions were taken for this key, return
		return true
	}

	if a, ok = bkt.m[k].(action); !ok {
		// No actions were taken for this key, return
		return true
	}

	if a.a == _del {
		return false
	}

	return true
}

// Put will put
func (txn *updateTxn) put(keys []string, k string, v Duper) (err error) {
	var (
		bkt *Bucket
		act = action{a: _put, v: v} // Create action
	)

	if len(k) > MaxKeyLen {
		err = ErrInvalidKey
		return
	}

	txn.mux.Lock()
	if bkt, err = createBucket(txn.a, keys, nil, nil); err == nil {
		if txn.h.opts.CopyOnWrite {
			// Create copy before inserting
			act.v = act.v.Dup()
		}

		bkt.m[k] = act
	}
	txn.mux.Unlock()

	return
}

// Del will delete
func (txn *updateTxn) delete(keys []string, k string) (err error) {
	var (
		bkt *Bucket
		act = action{a: _del} // Create action
	)

	if len(k) > MaxKeyLen {
		return
	}

	txn.mux.Lock()
	if bkt = txn.a.bucket(keys); bkt == nil {
		if bkt = txn.h.root.bucket(keys); bkt == nil {
			goto END
		}
	}

	bkt.m[k] = act

END:
	txn.mux.Unlock()
	return
}

// forEach will iterate through each item
func (txn *updateTxn) forEach(keys []string, fn ForEachFn) (err error) {
	var (
		bkt    *Bucket
		abkt   *Bucket
		ignore = make(map[string]struct{})
	)

	// We are going to retreive the bucket with matching keys
	if bkt = txn.h.root.bucket(keys); bkt == nil {
		// Nothing was found, return!
		return
	}

	if abkt = txn.a.bucket(keys); bkt == nil {
		return bkt.forEach(fn)
	}

	for k, v := range abkt.m {
		a, ok := v.(action)
		if !ok {
			continue
		}

		ignore[k] = struct{}{}
		if a.a == _del {
			continue
		}

		if txn.h.opts.CopyOnRead {
			fn(k, a.v.Dup())
		} else {
			fn(k, a.v)
		}
	}

	return bkt.forEachExcept(ignore, fn)
}
