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
