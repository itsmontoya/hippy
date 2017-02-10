package hippy

type txn uint8

const (
	rTxn txn = iota
	rwTxn
	wTxn
)

// Duper returns a duplicate of itself
type Duper interface {
	Dup() Duper
}

// RawValue value type
type RawValue []byte

// MarshalFn is the marshaling func used by Value
type MarshalFn func(interface{}) ([]byte, error)

// UnmarshalFn is the unmarshaling func used by Value
type UnmarshalFn func([]byte) (interface{}, error)

// action stores the action-type and body for a transaction item
type action struct {
	a byte
	v Duper
}

type storage map[string]*Bucket

func reverseByteSlice(bs []byte) {
	var n int
	mc := len(bs) - 1
	for i := range bs {
		if n = mc - i; n == i || n < i {
			break
		}

		bs[i], bs[n] = bs[n], bs[i]
	}
}

// ForEachFn is used for ForEach requests
type ForEachFn func(key string, val interface{}) error

// TxnFn is used for transactions
type TxnFn func(txn Txn) error

func createBucket(bkt *Bucket, keys []string, mfn MarshalFn, ufn UnmarshalFn) (nbkt *Bucket, err error) {
	nbkt = bkt

	for i, k := range keys {
		if len(k) > MaxKeyLen {
			nbkt = nil
			err = ErrInvalidKey
			return
		}

		switch tgt := bkt.m[k].(type) {
		case *Bucket:
			nbkt = tgt

		case action:
			if tgt.a == _put {
				err = ErrCannotCreateBucket
				return
			}

			nbkt = bktP.Get()
			nbkt.keys = keys[:i+1]
			nbkt.txn = bkt.txn

		case nil:
			nbkt = bktP.Get()
			nbkt.keys = keys[:i+1]
			nbkt.txn = bkt.txn
		}

		bkt.m[k] = nbkt
		bkt = nbkt
	}

	if nbkt.mfn == nil {
		nbkt.mfn = mfn
		nbkt.ufn = ufn
		nbkt.parseRaw()
	}

	return
}
