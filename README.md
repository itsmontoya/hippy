# Hippy [![GoDoc](https://godoc.org/github.com/itsmontoya/hippy?status.svg)](https://godoc.org/github.com/itsmontoya/hippy) ![Status](https://img.shields.io/badge/status-alpha-red.svg)
Hippy is an in-memory database with aspirations to be ACID compliant.


## Benchmarks
```
# Short (1:1 Get/Put ratio, 1 iteration per operation)
BenchmarkShortHippy-4       2000            886459 ns/op           12490 B/op         60 allocs/op
BenchmarkShortLMap-4     1000000              2286 ns/op             960 B/op         10 allocs/op
BenchmarkShortBolt-4        2000            880754 ns/op           19688 B/op         72 allocs/op

# Basic (1:1 Get/Put ratio, 100 iterations per operation)
BenchmarkBasicHippy-4       2000            885972 ns/op           12489 B/op         60 allocs/op
BenchmarkBasicLMap-4       10000            225378 ns/op           96001 B/op       1000 allocs/op
BenchmarkBasicBolt-4        2000           1138923 ns/op           84241 B/op       3042 allocs/op

# All Get (1:0 Get/Put ratio, 100 iterations per operation)
BenchmarkAllGetHippy-4    200000             11162 ns/op               0 B/op          0 allocs/op
BenchmarkAllGetLMap-4      20000             77227 ns/op           96000 B/op       1000 allocs/op
BenchmarkAllGetBolt-4      10000            124850 ns/op           32384 B/op       1005 allocs/op

# Middleware testing (Benching only available for Hippy)
BenchmarkGzipHippy-4         300           4983737 ns/op        14588356 B/op        584 allocs/op
BenchmarkCryptyHippy-4      2000            911470 ns/op           20017 B/op        140 allocs/op

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

	// Create new db, with a path of "./" and a name of "data"
	// Note: This will produce a file at "./data.hdb"
	if db, err = hippy.New("./", "data"); err != nil {
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
