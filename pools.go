package hippy

import "sync"

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
