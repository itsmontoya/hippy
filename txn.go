package hippy

// Txn is a transaction
type Txn interface {
	Bucket(keys ...string) *Bucket
	CreateBucket(key string, mfn MarshalFn, ufn UnmarshalFn) (*Bucket, error)
	DeleteBucket(keys ...string) error
	Buckets() []string

	bucket(keys []string) *Bucket
	createBucket(keys []string, mfn MarshalFn, ufn UnmarshalFn) (*Bucket, error)
	deleteBucket(keys []string) error
	buckets(keys []string) []string

	get(keys []string, key string) Duper
	has(keys []string, key string) bool
	put(keys []string, key string, val Duper) error
	delete(keys []string, key string) error
	forEach(keys []string, fn ForEachFn) error
}
