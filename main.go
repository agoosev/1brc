package main

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"unsafe"

	"golang.org/x/sys/unix"
)

type cityMeasurement struct {
	name  string
	hash  uint64
	min   float64
	max   float64
	total float64
	count float64
}

const (
	fnv1aOffset uint64 = 0xcbf29ce484222325
	fnv1aPrime  uint64 = 0x100000001b3

	storageCapacity = 16384
)

func main() {
	if len(os.Args) < 2 {
		log.Fatal("missed measurements file path")
	}

	res, err := process(os.Args[1])
	if err != nil {
		log.Fatalf("processing error: %s", err)
	}

	fmt.Println(res)
}

func process(filePath string) (string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", fmt.Errorf("could not open measurements file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return "", fmt.Errorf("file stat error: %w", err)
	}

	data, err := unix.Mmap(int(f.Fd()), 0, int(stat.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return "", fmt.Errorf("mmap file error: %w", err)
	}
	defer unix.Munmap(data)

	measurements, err := getMeasurements(data)
	if err != nil {
		return "", fmt.Errorf("unable to receive measurements: %w", err)
	}

	result := make([]string, 0, len(measurements))

	cities := make([]string, 0, len(measurements))

	for _, m := range measurements {
		if m != nil {
			cities = append(cities, m.name)
		}
	}

	sort.Strings(cities)

	for _, city := range cities {
		index, ok := getIndex(stringHash(city), measurements)
		if !ok {
			return "", fmt.Errorf("city not found")
		}
		m := measurements[index]

		result = append(result, fmt.Sprintf("%s=%.1f/%.1f/%.1f", city, m.min, round(m.total/m.count), m.max))
	}

	return "{" + strings.Join(result, ", ") + "}\n", nil
}

func getMeasurements(data []byte) ([]*cityMeasurement, error) {
	var wg sync.WaitGroup

	workersCount := runtime.GOMAXPROCS(0)
	chunkSize := len(data) / workersCount
	if chunkSize == 0 {
		chunkSize = len(data)
	}
	borders := getBorders(data, chunkSize, workersCount)

	start := 0
	results := make([][]*cityMeasurement, len(borders))
	for i, border := range borders {
		wg.Add(1)

		go func(workerNumber, start, end int) {
			results[workerNumber] = processChunk(data[start:end])
			wg.Done()
		}(i, start, border)

		start = border
	}

	wg.Wait()
	results = filterResults(results)

	if len(results) > 1 {
		for _, storage := range results[1:] {
			for _, m := range storage {
				if m == nil {
					continue
				}

				index, ok := getIndex(m.hash, results[0])
				if !ok {
					results[0][index] = m
				} else {
					v := results[0][index]
					v.min = min(v.min, m.min)
					v.max = max(v.max, m.max)
					v.total += m.total
					v.count += m.count
					results[0][index] = v
				}
			}
		}

	}

	return results[0], nil
}

func processChunk(data []byte) []*cityMeasurement {
	result := make([]*cityMeasurement, storageCapacity)

	start := 0
	semicolumnPos := 0
	calculateHash := true
	hash := fnv1aOffset
	for i, v := range data {
		if v == ';' {
			semicolumnPos = i
			calculateHash = false
			continue
		}

		if calculateHash {
			hash ^= uint64(data[i])
			hash *= fnv1aPrime
		}

		if v != '\n' {
			continue
		}

		city := bytesToString(data[start:semicolumnPos])

		value := parseFloat64(data[semicolumnPos+1 : i])

		start = i + 1

		index, ok := getIndex(hash, result)
		if !ok {
			result[index] = &cityMeasurement{
				name:  city,
				hash:  hash,
				min:   value,
				max:   value,
				total: value,
				count: 1,
			}
			calculateHash = true
			hash = fnv1aOffset

			continue
		}

		m := result[index]
		m.min = min(m.min, value)
		m.max = max(m.max, value)
		m.total += value
		m.count++

		result[index] = m

		calculateHash = true
		hash = fnv1aOffset
	}

	return result
}

func getIndex(hash uint64, s []*cityMeasurement) (uint64, bool) {
	index := hash & (storageCapacity - 1)

	for s[index] != nil && s[index].hash != hash {
		index = (index + 1) & (storageCapacity - 1)
	}

	return index, s[index] != nil
}

func round(x float64) float64 {
	x = x * 10
	truncated := math.Trunc(x)
	if math.Abs(x-truncated) >= 0.1 {
		truncated += math.Copysign(1, x)
	}

	return truncated / 10.0
}

func parseFloat64(b []byte) float64 {
	var (
		sign   float64 = 1
		result float64
	)

	if b[0] == '-' {
		sign = -1
		b = b[1:]
	}

	if len(b) == 3 {
		result = float64(b[0]-'0') + float64(b[2]-'0')*0.1
	} else {
		result = float64(b[0]-'0')*10 + float64(b[1]-'0') + float64(b[3]-'0')*0.1
	}

	return sign * result
}

func bytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
}

func stringHash(s string) uint64 {
	hash := fnv1aOffset
	for _, b := range []byte(s) {
		hash ^= uint64(b)
		hash *= fnv1aPrime
	}

	return hash
}

func getBorders(b []byte, chunkSize int, maxChunksCount int) []int {
	border := 0
	borders := make([]int, 0, maxChunksCount)
	for border < len(b) {
		border += chunkSize
		if border > len(b)-1 {
			borders = append(borders, len(b))
			break
		}

		eolPosition := bytes.IndexByte(b[border:], '\n')
		if eolPosition == -1 {
			borders = append(borders, len(borders))
			break
		}

		border = border + eolPosition + 1
		borders = append(borders, border)
	}

	return borders
}

func filterResults(s [][]*cityMeasurement) [][]*cityMeasurement {
	result := make([][]*cityMeasurement, 0, len(s))
	for _, v := range s {
		if len(v) != 0 {
			result = append(result, v)
		}
	}

	return result
}
