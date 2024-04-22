#!/bin/bash

mkdir profiles/$1
go test -run '^$' -bench '^Benchmark_process$' -benchtime 10s -count 6 -cpu 4 -cpuprofile=profiles/$1/cpu.pprof