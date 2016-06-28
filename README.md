# Hippy [![GoDoc](https://godoc.org/github.com/itsmontoya/hippy?status.svg)](https://godoc.org/github.com/itsmontoya/hippy) ![Status](https://img.shields.io/badge/status-alpha-red.svg)
Hippy is an in-memory database with aspirations to be ACID compliant.


## Benchmarks
```
BenchmarkShortHippy-4       2000            992290 ns/op             323 B/op         30 allocs/op
BenchmarkBasicHippy-4       2000            889997 ns/op            8245 B/op       1020 allocs/op
BenchmarkAllGetHippy-4     30000             52624 ns/op            8001 B/op       1000 allocs/op

BenchmarkShortBolt-4        2000            872558 ns/op           15840 B/op         68 allocs/op
BenchmarkBasicBolt-4        2000           1088854 ns/op           80393 B/op       3038 allocs/op
BenchmarkAllGetBolt-4      10000            120124 ns/op           32384 B/op       1005 allocs/op

BenchmarkShortLMap-4     1000000              2268 ns/op              80 B/op         10 allocs/op
BenchmarkBasicLMap-4       10000            177234 ns/op            8000 B/op       1000 allocs/op
BenchmarkAllGetLMap-4      20000             58410 ns/op            8000 B/op       1000 allocs/op
```

## Usage
```go
package main

import (
        "fmt"
        "errors"
		"time"
		
		"github.com/itsmontoya/hippy"
)

var ErrNoName = errors.New("no net is set")

func main(){
	var (
			db *hippy.Hippy
			err error
	)

	// Create new db, with the location referencing "./test.db"
	if db, err = hippy.New("./test.db"); err != nil {
			fmt.Println("Error opening:", err)
			return
	}
	
	go func(){
		time.Sleep(time.Second * 3)
		db.ReadWrite(setName)
	}()
	
	// Continue to loop until we no longer encounter an error
	for db.Read(checkName) != nil {
		// Sleep for a second so we don't burn out the CPU
		time.Sleep(time.Second)
	}
}

func setName(tx *hippy.ReadWriteTx) (err error) {
	// Set key of "name" with a value of "John Doe" (represented as a byte-slice)
	tx.Put("name", []byte("John Doe"))
	return
}

func checkName(tx *hippy.ReadTx) (err error) {
	// Get value set for the key of "name"
	if name, ok := tx.Get("name"); ok {
		fmt.Println("Name is", string(name))
	} else {
		// Name does not exist, set error to ErrNoName
		fmt.Println("Name does not exist")
		err = ErrNoName
	}
	
	return
}
```
