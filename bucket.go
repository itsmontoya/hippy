package hippy

import "sync"

// RawValue value type
type RawValue []byte

var bktP = bucketPool{
	p: sync.Pool{
		New: func() interface{} {
			return newBucket()
		},
	},
}

type bucketPool struct {
	p sync.Pool
}

func (b *bucketPool) Get() (bkt *Bucket) {
	bkt, _ = b.p.Get().(*Bucket)
	return
}

func (b *bucketPool) Put(bkt *Bucket) {
	bkt.keys = bkt.keys[:]
	bkt.mfn = nil
	bkt.ufn = nil
	bkt.txn = nil

	for k := range bkt.m {
		delete(bkt.m, k)
	}

	b.p.Put(bkt)
}

func newBucket() *Bucket {
	return &Bucket{
		m: make(map[string]interface{}),
	}
}

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
			bkt = nil
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

		nb := bktP.Get()
		nb.keys = keys[:i+1]
		nb.txn = b.txn

		bkt.m[k] = nb
		bkt = nb
	}

	return
}

func (b *Bucket) parseRaw() (err error) {
	if b.ufn == nil {
		return
	}

	var (
		rv RawValue
		ok bool
	)

	for k, v := range b.m {
		if rv, ok = v.(RawValue); !ok {
			continue
		}

		if v, err = b.ufn(rv[:]); err != nil {
			continue
		}

		b.m[k] = v
	}

	return
}

// CreateBucket will create a bucket
func (b *Bucket) CreateBucket(key string, mfn MarshalFn, ufn UnmarshalFn) (bkt *Bucket, err error) {
	if bkt, err = b.createBucket([]string{key}); err != nil {
		return
	}

	if bkt.mfn != nil {
		return
	}

	bkt.mfn = mfn
	bkt.ufn = ufn
	bkt.txn = b.txn
	return
}

// DeleteBucket will delete a bucket
func (b *Bucket) DeleteBucket(key string) (err error) {
	var (
		rw  *ReadWriteTx
		bkt *Bucket
		bk  []string

		delKeys = make(map[string]struct{})
		delBkts = make(map[string]struct{})
	)

	switch txn := b.txn.(type) {
	case *ReadWriteTx:
		rw = txn

	default:
		err = ErrInvalidTxnType
		return
	}

	bk = append(bk, b.keys...)
	bk = append(bk, key)

	if bkt = rw.h.root.bucket(bk); bkt != nil {
		for _, k := range bkt.Buckets() {
			delBkts[k] = struct{}{}
		}

		for _, k := range bkt.Keys() {
			delKeys[k] = struct{}{}
		}
	}

	if bkt = b.bucket([]string{key}); bkt != nil {
		for _, k := range bkt.Buckets() {
			delBkts[k] = struct{}{}
		}

		for _, k := range bkt.Keys() {
			delKeys[k] = struct{}{}
		}
	}

	for k := range delBkts {
		b.DeleteBucket(k)
	}

	kend := len(bk)
	keys := make([]string, kend+1)
	copy(keys, bk)

	for k := range delKeys {
		keys[kend] = k
		rw.del(keys)
	}

	return
}

// Get will return an interface matching a provided key
func (b *Bucket) Get(k string) (v interface{}) {
	switch txn := b.txn.(type) {
	case *ReadWriteTx:
		v = txn.get(append(b.keys, k))

	case *ReadTx, nil:
		v = b.m[k]
	}

	return
}

// Has will return a boolean representing if there is a match for a provided key
func (b *Bucket) Has(k string) (ok bool) {
	_, ok = b.m[k]
	return
}

// Put will put an interface for a provided key
func (b *Bucket) Put(k string, v interface{}) (err error) {
	switch txn := b.txn.(type) {
	case *ReadWriteTx:
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

// ForEach will iterate through each item within a bucket
func (b *Bucket) ForEach(fn func(k string, v interface{}) error) (err error) {
	for k, v := range b.m {
		if _, ok := v.(*Bucket); ok {
			continue
		}

		if err = fn(k, v); err != nil {
			break
		}
	}

	return

}

// Keys will list all the keys for a particular bucket
func (b *Bucket) Keys() (ks []string) {
	switch txn := b.txn.(type) {
	case *ReadTx, nil:
		for k := range b.m {
			ks = append(ks, k)
		}

	case *ReadWriteTx:
		for k, v := range b.m {
			if _, ok := v.(*Bucket); ok {
				continue
			}

			ks = append(ks, k)

		}

		if bkt := txn.h.root.bucket(b.keys); bkt != nil {
			for k, v := range bkt.m {
				if _, ok := v.(*Bucket); !ok {
					continue
				}

				ks = append(ks, k)
			}
		}
	}

	return
}

// Buckets will return a list of buckets contained directly within this bucket
// Note: Children of child buckets are not included in this list
func (b *Bucket) Buckets() (bs []string) {
	for k, v := range b.m {
		switch v.(type) {
		case *Bucket:
			bs = append(bs, k)
		}
	}

	if rw, ok := b.txn.(*ReadWriteTx); ok {
		var bkt *Bucket
		if bkt = rw.h.root.bucket(b.keys); bkt == nil {
			return
		}

		bm := make(map[string]struct{}, len(bs))
		for _, k := range bs {
			bm[k] = struct{}{}
		}

		for _, k := range bkt.Buckets() {
			if _, ok := bm[k]; ok {
				continue
			}

			bs = append(bs, k)
		}
	}

	return
}
