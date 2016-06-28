package main

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/itsmontoya/hippy"
	"github.com/pkg/profile"
)

func main() {
	var (
		db  *hippy.Hippy
		err error

		bb []byte

		testKeys = []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}
		testVal  = []byte("Hello!")

		done int64
	)

	if db, err = hippy.New("test.db"); err != nil {
		fmt.Println("Error opening:", err)
		return
	}

	p := profile.Start(profile.CPUProfile, profile.ProfilePath("."), profile.NoShutdownHook)

	go func() {
		time.Sleep(time.Minute * 3)
		atomic.SwapInt64(&done, 1)
	}()

	for atomic.LoadInt64(&done) == 0 {
		db.ReadWrite(func(txn *hippy.ReadWriteTx) (err error) {
			for i := 0; i < 1000; i++ {
				for _, k := range testKeys {
					txn.Put(k, testVal)
					bb, _ = txn.Get(k)
				}
			}
			return
		})
	}

	p.Stop()
}
