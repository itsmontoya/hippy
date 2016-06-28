# Hippy [![GoDoc](https://godoc.org/github.com/itsmontoya/hippy?status.svg)](https://godoc.org/github.com/itsmontoya/hippy) ![Status](https://img.shields.io/badge/status-alpha-red.svg)
Hippy is an in-memory database with aspirations to be ACID compliant.


## Benchmarks
```
BenchmarkShortHippy-4       2000            873541 ns/op             323 B/op         30 allocs/op
BenchmarkBasicHippy-4       2000            873778 ns/op            8245 B/op       1020 allocs/op
BenchmarkAllGetHippy-4     50000             37352 ns/op            8001 B/op       1000 allocs/op

BenchmarkShortBolt-4        2000            870014 ns/op           15840 B/op         68 allocs/op
BenchmarkBasicBolt-4        2000           1103311 ns/op           80393 B/op       3038 allocs/op
BenchmarkAllGetBolt-4      10000            120361 ns/op           32384 B/op       1005 allocs/op

BenchmarkShortLMap-4     1000000              1288 ns/op               0 B/op          0 allocs/op
BenchmarkBasicLMap-4       10000            127836 ns/op               0 B/op          0 allocs/op
BenchmarkAllGetLMap-4      30000             50101 ns/op               0 B/op          0 allocs/op
```
