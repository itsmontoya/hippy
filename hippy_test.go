package hippy

import (
	//	"encoding/json"
	"errors"
	"fmt"
	//"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	//"time"

	"github.com/boltdb/bolt"
	//"github.com/itsmontoya/middleware"
)

var (
	db  *Hippy
	bdb *bolt.DB
	mdb *LMap

	boltBktKey = []byte("temp")

	testKeys  = []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
	testKeysB = getKeysB(testKeys)
	testVal   = []byte("Hello! This is my long-ish string. It has some cool information in it. Check it out man!")
	testValD  = TestSliceDuper(testVal)
	tmpPath   = "./testing/"

	opts Opts

	cryptyKey = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31}
	cryptyIV  = []byte{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}

	mfn = func(v interface{}) (b []byte, err error) {
		b, _ = v.([]byte)
		return
	}

	ufn = func(b []byte) (v interface{}, err error) {
		v = b
		return
	}

	outB []byte

	ErrInvalidValueType = errors.New("invalid value type")
)

type TestEntry struct {
	Name string
	DOB  string
}

type TestStrDuper string

func (t TestStrDuper) Dup() Duper {
	return t
}

type TestSliceDuper []byte

func (t TestSliceDuper) Dup() Duper {
	dt := make(TestSliceDuper, len(t))
	copy(dt, t)
	return dt
}

func getKeysB(keys []string) (out [][]byte) {
	for _, v := range keys {
		out = append(out, []byte(v))
	}
	return
}

/*
func TestMain(m *testing.M) {
	var err error

	opts, _ = NewOpts(nil)

	if tmpPath, err = ioutil.TempDir("", "hippy_testing_"); err != nil {
		fmt.Println("Error getting temp dir:", err)
		return
	}

	tmpPath = "testing"

	if db, err = New(tmpPath, "test", opts, vv); err != nil {
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
*/

func TestBasic(t *testing.T) {
	var (
		db  *Hippy
		err error
	)

	if db, err = New(NewDefaultOpts(tmpPath, "basic_test")); err != nil {
		fmt.Println("Error opening:", err)
		return
	}

	db.Update(func(tx Txn) (err error) {
		var bkt *Bucket
		if bkt, err = tx.CreateBucket("main", mfn, ufn); err != nil {
			return
		}

		bkt.Put("greeting", TestStrDuper("hai"))

		if str, ok := bkt.Get("greeting").(string); ok {
			fmt.Println("GET", str)
		} else {
			fmt.Println("Get error", bkt.Get("greeting"))
		}

		return
	})

	db.Update(func(tx Txn) (err error) {
		var bkt *Bucket
		tx.DeleteBucket("main")

		if bkt, err = tx.CreateBucket("main", mfn, ufn); err != nil {
			return
		}

		if str, ok := bkt.Get("greeting").(string); !ok {
			fmt.Println("Value does not exist as intended :)")
		} else {
			fmt.Println("Value exists when it should not", str)
		}

		return
	})

	/*
		db.Update(func(tx Txn) (err error) {
			bkt := tx.Bucket("main")
			fmt.Println("Bucket?", bkt)
			if str, ok := bkt.Get("greeting").(string); ok {
				fmt.Println("GET", str)
			} else {
				fmt.Println("Get error", bkt.Get("greeting"))
			}
			return
		})
	*/
	db.Close()
	removeFile("basic_test.hdb")
	removeFile("basic_test.archive.hdb")
}

func removeFile(name string) error {
	return os.Remove(filepath.Join(tmpPath, name))
}

/*
func BenchmarkShortHippy(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			hippyRW(db, 1)

		}
	})

	b.ReportAllocs()
}
*/

func BenchmarkBasicHippy(b *testing.B) {
	var (
		db  *Hippy
		err error
	)

	b.StopTimer()
	if db, err = New(NewDefaultOpts(tmpPath, "basic_test")); err != nil {
		b.Error("Error opening:" + err.Error())
		return
	}

	if err = db.Update(func(tx Txn) (err error) {
		tx.CreateBucket("main", mfn, ufn)
		return
	}); err != nil {
		b.Error("Error setting bucket:" + err.Error())
		return
	}

	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var err error
			if _, err = hippyRW(db, 100); err != nil {
				b.Error(err)
			}
		}
	})

	b.ReportAllocs()
	b.StopTimer()
	db.Close()
}

func BenchmarkBasicBolt(b *testing.B) {
	var (
		db  *bolt.DB
		err error
	)

	b.StopTimer()
	if db, err = bolt.Open(filepath.Join(tmpPath, "test.bdb"), 0644, nil); err != nil {
		fmt.Println("Error opening:", err)
	}

	db.Update(func(tx *bolt.Tx) error {
		tx.CreateBucket([]byte("main"))
		return nil
	})

	b.StartTimer()
	b.RunParallel(func(pb *testing.PB) {
		var err error
		for pb.Next() {
			if _, err = boltRW(db, 100); err != nil {
				b.Error(err)
			}
		}
	})

	b.ReportAllocs()
	b.StopTimer()
	db.Close()
}

/*
func BenchmarkGzipHippy(b *testing.B) {
	b.StopTimer()
	var (
		db  *Hippy
		err error
	)

	if db, err = New(tmpPath, "gzip", opts, vv, middleware.GZipMW{}); err != nil {
		b.Error("Error opening:", err)
		return
	}
	b.StartTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			hippyRW(db, 100)

		}
	})

	b.ReportAllocs()
	b.StopTimer()
	db.Close()
	os.Remove(filepath.Join(tmpPath, "gzip.hdb"))
}

func BenchmarkCryptyHippy(b *testing.B) {
	b.StopTimer()
	var (
		db  *Hippy
		err error
	)

	if db, err = New(tmpPath, "crypty", opts, vv, middleware.NewCryptyMW(cryptyKey, cryptyIV)); err != nil {
		b.Error("Error opening:", err)
		return
	}
	b.StartTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			hippyRW(db, 100)

		}
	})

	b.ReportAllocs()
	db.Close()
	os.Remove(filepath.Join(tmpPath, "crypty.hdb"))
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


func BenchmarkAllGetBolt(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			boltR(bdb, 100)
		}
	})
	b.ReportAllocs()
}


func hippyR(db *Hippy, iter int) (err error) {
	var (
		bb []byte
		ok bool
		v  interface{}
	)

	return db.Read(func(txn *ReadTx) (err error) {
		for i := 0; i < iter; i++ {
			for _, k := range testKeys {
				v, _ = txn.Get(k)
				bb, ok = v.([]byte)
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

*/

func hippyRW(db *Hippy, iter int) (b []byte, err error) {
	var ok bool
	err = db.Update(func(txn Txn) (err error) {
		bkt := txn.Bucket("main")

		for i := 0; i < iter; i++ {
			for _, k := range testKeys {
				bkt.Put(k, testValD)
				if b, ok = bkt.Get(k).(TestSliceDuper); !ok {
					err = ErrInvalidValueType
					return
				}
			}
		}

		return
	})

	return
}

func boltRW(bdb *bolt.DB, iter int) (bb []byte, err error) {
	err = bdb.Update(func(tx *bolt.Tx) (err error) {
		bkt := tx.Bucket([]byte("main"))
		for i := 0; i < iter; i++ {
			for _, k := range testKeysB {
				bkt.Put(k, testVal)
				bb = bkt.Get(k)
			}
		}
		return
	})

	return
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
