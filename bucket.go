package hippy

import "fmt"

// RawValue value type
type RawValue []byte

// Bucket manages buckets of data
type Bucket struct {
	keys []string

	mfn MarshalFn
	ufn UnmarshalFn

	txn interface{}
	m   map[string]interface{}
}

func (b *Bucket) bucket(keys []string) (bkt *Bucket) {
	var (
		v  interface{}
		ok bool
	)

	bkt = b

	for _, k := range keys {
		if v, ok = bkt.m[k]; !ok {
			return
		}

		if bkt, ok = v.(*Bucket); !ok {
			bkt = nil
			return
		}
	}

	return
}

func (b *Bucket) createBucket(keys []string) (bkt *Bucket, err error) {
	var (
		v  interface{}
		ok bool
		kl int
	)

	bkt = b
	if kl = len(keys); kl == 0 {
		return
	}

	for i, k := range keys {
		if len(k) > MaxKeyLen {
			bkt = nil
			err = ErrInvalidKey
			return
		}

		if v, ok = bkt.m[k]; ok {
			if bkt, ok = v.(*Bucket); !ok {
				err = ErrCannotCreateBucket
				return
			}

			continue
		}

		nb := &Bucket{
			keys: keys[:i+1],
			m:    make(map[string]interface{}),
			txn:  b.txn,
		}

		bkt.m[k] = nb
		bkt = nb
	}

	return
}

// CreateBucket will create a bucket
func (b *Bucket) CreateBucket(key string, mfn MarshalFn, ufn UnmarshalFn) (err error) {
	var (
		bkt *Bucket
		ok  bool
	)

	if bkt, err = b.createBucket([]string{key}); err != nil {
		return
	}

	if bkt.mfn != nil {
		return
	}

	bkt.mfn = mfn
	bkt.ufn = ufn
	bkt.txn = b.txn

	var rv RawValue
	for k, v := range bkt.m {
		if rv, ok = v.(RawValue); !ok {
			continue
		}

		if v, err = ufn(rv[:]); err != nil {
			continue
		}

		bkt.m[k] = v
	}

	return
}

// Get will return an interface matching a provided key
func (b *Bucket) Get(k string) (v interface{}) {
	fmt.Println("Getting", k)
	switch txn := b.txn.(type) {
	case *ReadTx:
		v = txn.get(append(b.keys, k))
	case *ReadWriteTx:
		v = txn.get(append(b.keys, k))
	}

	return
}

// Has will return a boolean representing if there is a match for a provided key
func (b *Bucket) Has(k string) (ok bool) {
	return
}

// Put will put an interface for a provided key
func (b *Bucket) Put(k string, v interface{}) (err error) {
	fmt.Println("Bucket put", b.keys, k, v)
	switch txn := b.txn.(type) {
	case *ReadWriteTx:
		fmt.Println("Txn put")
		err = txn.put(append(b.keys, k), v)

	default:
		err = ErrInvalidTxnType
	}

	return
}

// Del will delete an interface matching a provided key
func (b *Bucket) Del(k string) {
	switch txn := b.txn.(type) {
	case ReadWriteTx:
		txn.del(append(b.keys, k))
		//	case WriteTx:
		//		err = txn.del(append(b.keys, key))
	}

	return
}

// Keys will list all the keys for a particular bucket
func (b *Bucket) Keys() (ks []string) {
	switch txn := b.txn.(type) {
	case ReadTx:
		ks = txn.keys(b.keys)
	case ReadWriteTx:
		ks = txn.keys(b.keys)
	}

	return
}

/*
	Get(k string) (v interface{}, ok bool)
	Put(k string, v interface{}) (err error)
	Del(k string)
	Keys() (ks []string)
*/
