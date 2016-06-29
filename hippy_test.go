package hippy

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/boltdb/bolt"
)

var (
	db  *Hippy
	bdb *bolt.DB
	mdb *LMap

	boltBktKey = []byte("temp")

	testKeys  = []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	testKeysB = getKeysB(testKeys)
	testVal   = []byte("Hello!")

	tmpPath string
)

func getKeysB(keys []string) (out [][]byte) {
	for _, v := range keys {
		out = append(out, []byte(v))
	}
	return
}

func TestMain(m *testing.M) {
	var err error

	if tmpPath, err = ioutil.TempDir("", "hippy_testing_"); err != nil {
		fmt.Println("Error getting temp dir:", err)
		return
	}

	tmpPath = "testing"

	if db, err = New(tmpPath, "test"); err != nil {
		fmt.Println("Error opening:", err)
		return
	}

	if bdb, err = bolt.Open(filepath.Join(tmpPath, "test.bdb"), 0644, nil); err != nil {
		fmt.Println("Error opening:", err)
	}

	mdb = &LMap{
		m: make(map[string][]byte),
	}

	bdb.Update(func(tx *bolt.Tx) error {
		tx.CreateBucket(boltBktKey)
		return nil
	})

	sts := m.Run()
	bdb.Close()
	//	if err = db.Close(); err != nil {
	//		fmt.Println("Error closing hippy:", err)
	//	}

	//	os.RemoveAll(tmpPath)
	os.Exit(sts)
}

func TestBasic(t *testing.T) {
	var (
		db  *Hippy
		err error
	)

	if db, err = New(tmpPath, "basic_test"); err != nil {
		fmt.Println("Error opening:", err)
		return
	}

	hippyRW(db, 1)
	db.Close()
}

func TestMWBasic(t *testing.T) {
	var (
		db  *Hippy
		err error
	)

	if db, err = New(tmpPath, "basicMW_test", GZipMW{}); err != nil {
		fmt.Println("Error opening:", err)
		return
	}

	hippyRW(db, 1)
	fmt.Println(db.Close())
}

func TestMedium(t *testing.T) {
	var (
		b   []byte
		ok  bool
		db  *Hippy
		err error
	)

	if db, err = New(tmpPath, "medium_test"); err != nil {
		fmt.Println("Error opening:", err)
		return
	}

	db.ReadWrite(func(txn *ReadWriteTx) (err error) {
		b, ok = txn.Get("greeting")
		//	fmt.Println(string(b), ok)

		txn.Put("greeting", []byte(`Hello!`))
		b, ok = txn.Get("greeting")
		return
	})

	db.ReadWrite(func(txn *ReadWriteTx) (err error) {
		txn.Put("greeting", []byte("NO!!"))
		b, ok = txn.Get("greeting")
		return errors.New("Merp")
	})

	db.ReadWrite(func(txn *ReadWriteTx) (err error) {
		if _, ok = txn.Get("greeting"); !ok {
			t.Error("key isn't found")
		}
		return
	})

	db.ReadWrite(func(txn *ReadWriteTx) (err error) {
		txn.Del("greeting")
		return
	})

	db.ReadWrite(func(txn *ReadWriteTx) (err error) {
		if _, ok = txn.Get("greeting"); ok {
			t.Error("key was found")
		}
		return
	})

	db.ReadWrite(func(txn *ReadWriteTx) (err error) {
		txn.Put("greeting", []byte("Hello!"))
		return
	})

	db.Close()
	//	os.Remove("basic_test.db")
}

func BenchmarkShortHippy(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			hippyRW(db, 1)

		}
	})

	b.ReportAllocs()
}

func BenchmarkBasicHippy(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			hippyRW(db, 100)

		}
	})

	b.ReportAllocs()
}

func BenchmarkAllGetHippy(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			hippyR(db, 100)

		}
	})

	b.ReportAllocs()
}

func BenchmarkShortLMap(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mapRW(mdb, 1)

		}
	})

	b.ReportAllocs()
}

func BenchmarkBasicLMap(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mapRW(mdb, 100)

		}
	})

	b.ReportAllocs()
}

func BenchmarkAllGetLMap(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			mapR(mdb, 100)

		}
	})

	b.ReportAllocs()
}

func BenchmarkShortBolt(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			boltRW(bdb, 1)
		}
	})
	b.ReportAllocs()
}

func BenchmarkBasicBolt(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			boltRW(bdb, 100)
		}
	})
	b.ReportAllocs()
}

func BenchmarkAllGetBolt(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			boltR(bdb, 100)
		}
	})
	b.ReportAllocs()
}

func hippyRW(db *Hippy, iter int) (err error) {
	var bb []byte
	return db.ReadWrite(func(txn *ReadWriteTx) (err error) {
		for i := 0; i < iter; i++ {
			for _, k := range testKeys {
				txn.Put(k, testVal)
				bb, _ = txn.Get(k)
			}
		}
		return
	})
}

func hippyR(db *Hippy, iter int) (err error) {
	var bb []byte
	return db.Read(func(txn *ReadTx) (err error) {
		for i := 0; i < iter; i++ {
			for _, k := range testKeys {
				bb, _ = txn.Get(k)
			}
		}
		return
	})
}

func mapRW(db *LMap, iter int) (err error) {
	var bb []byte
	for i := 0; i < iter; i++ {
		for _, k := range testKeys {
			db.Put(k, testVal)
			bb, _ = db.Get(k)
		}
	}

	if bb == nil {
		return
	}
	return
}

func mapR(db *LMap, iter int) (err error) {
	var bb []byte
	for i := 0; i < iter; i++ {
		for _, k := range testKeys {
			bb, _ = db.Get(k)
		}
	}

	if bb == nil {
		return
	}
	return
}

func boltRW(bdb *bolt.DB, iter int) (err error) {
	var bb []byte
	return bdb.Update(func(tx *bolt.Tx) (err error) {
		bkt := tx.Bucket(boltBktKey)
		for i := 0; i < iter; i++ {
			for _, k := range testKeysB {
				bkt.Put(k, testVal)
				bb = bkt.Get(k)
			}
		}
		return
	})
}

func boltR(bdb *bolt.DB, iter int) (err error) {
	var bb []byte
	return bdb.View(func(tx *bolt.Tx) (err error) {
		bkt := tx.Bucket(boltBktKey)
		for i := 0; i < iter; i++ {
			for _, k := range testKeysB {
				bb = bkt.Get(k)
			}
		}
		return
	})
}

type LMap struct {
	mux sync.RWMutex
	m   map[string][]byte
}

func (l *LMap) Get(k string) (b []byte, ok bool) {
	var tgt []byte
	l.mux.RLock()
	if tgt, ok = l.m[k]; ok {
		b = make([]byte, len(tgt))
		copy(b, tgt)
	}

	l.mux.RUnlock()
	return
}

func (l *LMap) Put(k string, v []byte) {
	l.mux.Lock()
	l.m[k] = v
	l.mux.Unlock()
}
