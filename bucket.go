package hippy

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

	txn Txn
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

// buckets will return a list of buckets contained directly within this bucket
// Note: Children of child buckets are not included in this list
func (b *Bucket) buckets() (bs []string) {
	for k, v := range b.m {
		if _, ok := v.(*Bucket); ok {
			bs = append(bs, k)
		}
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

// forEach will iterate through each Duper within a bucket
func (b *Bucket) forEach(fn ForEachFn) (err error) {
	var ok bool
	for k, v := range b.m {
		if _, ok = v.(Duper); !ok {
			continue
		}

		if err = fn(k, v); err != nil {
			break
		}
	}

	return
}

// forEach will iterate through each Duper within a bucket and NOT contained in the ignore map
func (b *Bucket) forEachExcept(ignore map[string]struct{}, fn ForEachFn) (err error) {
	var ok bool
	for k, v := range b.m {
		if _, ok = ignore[k]; ok {
			continue
		}

		if _, ok = v.(Duper); !ok {
			continue
		}

		if err = fn(k, v); err != nil {
			break
		}
	}

	return
}

// Bucket will return a bucket
func (b *Bucket) Bucket(keys ...string) (bkt *Bucket) {
	ks := make([]string, 0, len(keys)+len(b.keys))
	ks = append(ks, keys...)
	ks = append(ks, b.keys...)
	return b.txn.bucket(ks)
}

// CreateBucket will create a bucket
func (b *Bucket) CreateBucket(key string, mfn MarshalFn, ufn UnmarshalFn) (bkt *Bucket, err error) {
	ks := make([]string, 0, len(b.keys)+1)
	ks = append(ks, b.keys...)
	ks = append(ks, key)
	return b.txn.createBucket(ks, mfn, ufn)
}

// DeleteBucket will delete a bucket
func (b *Bucket) DeleteBucket(key string) (err error) {
	ks := make([]string, 0, len(b.keys)+1)
	ks = append(ks, b.keys...)
	ks = append(ks, key)
	return b.txn.deleteBucket(ks)
}

// Get will return an interface matching a provided key
func (b *Bucket) Get(k string) (v interface{}) {
	return b.txn.get(b.keys, k)
}

// Has will return a boolean representing if there is a match for a provided key
func (b *Bucket) Has(k string) bool {
	return b.txn.has(b.keys, k)
}

// Put will put an interface for a provided key
func (b *Bucket) Put(k string, v Duper) error {
	return b.txn.put(b.keys, k, v)
}

// Delete will delete an interface matching a provided key
func (b *Bucket) Delete(k string) error {
	return b.txn.delete(b.keys, k)
}

// ForEach will iterate through each item within a bucket
func (b *Bucket) ForEach(fn func(k string, v interface{}) error) (err error) {
	return b.txn.forEach(b.keys, fn)
}
