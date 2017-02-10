package hippy

import ()

// readTxn is a read-only transaction
type readTxn struct {
	// Pointer to our DB's internal store
	h *Hippy
}

// Bucket will retrieve a bucket
func (txn *readTxn) Bucket(keys ...string) (bkt *Bucket) {
	return txn.h.root.bucket(keys)
}

// CreateBucket will create a bucket
func (txn *readTxn) CreateBucket(_ string, _ MarshalFn, _ UnmarshalFn) (*Bucket, error) {
	return nil, ErrInvalidTxnType
}

// DeleteBucket will delete a bucket
func (txn *readTxn) DeleteBucket(keys ...string) error {
	return ErrInvalidTxnType
}

// Buckets will retrieve a list of buckets
func (txn *readTxn) Buckets() (bs []string) {
	return txn.h.root.buckets()
}

func (txn *readTxn) bucket(keys []string) *Bucket {
	return txn.h.root.bucket(keys)
}

func (txn *readTxn) createBucket(_ []string, _ MarshalFn, _ UnmarshalFn) (*Bucket, error) {
	return nil, ErrInvalidTxnType
}

func (txn *readTxn) deleteBucket(_ []string) error {
	return ErrInvalidTxnType
}

func (txn *readTxn) buckets(keys []string) (bs []string) {
	if bkt := txn.h.root.bucket(keys); bkt != nil {
		bs = bkt.buckets()
	}

	return
}

// get will get a value for the provided keys
func (txn *readTxn) get(keys []string, k string) (d Duper) {
	var (
		bkt *Bucket
		ok  bool
	)

	// We are going to retreive the bucket with matching keys
	// Note: We omit the last key, as this is for the value - rather than the buckets!
	if bkt = txn.h.root.bucket(keys); bkt == nil {
		// Nothing was found, return!
		return
	}

	// Try to get value at bucket matching our key
	if d, ok = bkt.m[k].(Duper); !ok {
		return
	}

	if txn.h.opts.CopyOnRead {
		d = d.Dup()
	}

	return

}

// has will return whether or not a key exists
func (txn *readTxn) has(keys []string, k string) (ok bool) {
	var bkt *Bucket

	// We are going to retreive the bucket with matching keys
	// Note: We omit the last key, as this is for the value - rather than the buckets!
	if bkt = txn.h.root.bucket(keys); bkt == nil {
		// Nothing was found, return!
		return
	}

	_, ok = bkt.m[k]
	return

}

// put will put a value for the provided keys
func (txn *readTxn) put(_ []string, _ string, _ Duper) error {
	return ErrInvalidTxnType
}

// delete will delete a value for the provided keys
func (txn *readTxn) delete(_ []string, _ string) error {
	return ErrInvalidTxnType
}

// forEach will iterate through each item
func (txn *readTxn) forEach(keys []string, fn ForEachFn) (err error) {
	var bkt *Bucket
	// We are going to retreive the bucket with matching keys
	if bkt = txn.h.root.bucket(keys); bkt == nil {
		// Nothing was found, return!
		return
	}

	return bkt.forEach(fn)
}
