package main

import (
	"bytes"
	"fmt"
	"hash/maphash"
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
	min   int32
	max   int32
	total int32
	count int32
}

const storageCapacity = 16384

var seed = maphash.MakeSeed()

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
		index, ok := getIndex(maphash.String(seed, city), measurements)
		if !ok {
			return "", fmt.Errorf("city not found")
		}
		m := measurements[index]

		min := prepareInt(m.min)
		avg := prepareInt(round(float64(m.total) / float64(m.count)))
		max := prepareInt(m.max)

		result = append(result, fmt.Sprintf("%s=%s/%s/%s", city, min, avg, max))
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
	i := 0
	for i < len(data) {
		word := *(*uint64)(unsafe.Pointer(&data[i]))

		semicolumnPos, ok := findPosition(word, ';', i)
		if !ok {
			i = i + 8
			continue
		}

		word = *(*uint64)(unsafe.Pointer(&data[semicolumnPos+1]))
		nlPos, ok := findPosition(word, '\n', semicolumnPos+1) // we 100% have new line in the next 8 bytes, or it's the last record in the file
		if !ok {
			nlPos = len(data) - 1
		}

		hash := maphash.Bytes(seed, data[start:semicolumnPos])

		city := bytesToString(data[start:semicolumnPos])

		value := parseInt32(data[semicolumnPos+1 : nlPos])

		i = nlPos + 1
		start = i

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

			continue
		}

		m := result[index]
		m.min = min(m.min, value)
		m.max = max(m.max, value)
		m.total += value
		m.count++

		result[index] = m
	}

	return result
}

func findPosition(word uint64, symbol byte, offset int) (int, bool) {
	var mask uint64 = 0x101010101010101 * uint64(symbol)
	xorResult := word ^ mask
	found := ((xorResult - 0x0101010101010101) &^ xorResult & 0x8080808080808080)
	if found == 0 {
		return 0, false
	}

	result := ((((found - 1) & 0x101010101010101) * 0x101010101010101) >> 56) - 1

	return int(result) + offset, true
}

func getIndex(hash uint64, s []*cityMeasurement) (uint64, bool) {
	index := hash & (storageCapacity - 1)

	for s[index] != nil && s[index].hash != hash {
		index = (index + 1) & (storageCapacity - 1)
	}

	return index, s[index] != nil
}

func round(x float64) int32 {
	rounded := math.Round(x)

	return int32(rounded)
}

func prepareInt(i int32) []byte {
	b := make([]byte, 0, 5)

	if i < 0 {
		b = append(b, '-')
		i *= -1
	}

	v := i / 10
	if v < 10 {
		b = append(b, '0'+byte(v))
	} else {
		b = append(b, '0'+byte(v/10), '0'+byte(v%10))
	}

	return append(b, '.', '0'+byte(i%10))
}

func parseInt32(b []byte) int32 {
	var (
		sign   int32 = 1
		result int32
	)

	if b[0] == '-' {
		sign = -1
		b = b[1:]
	}

	if len(b) == 3 {
		result = int32(b[0]-'0')*10 + int32(b[2]-'0')
	} else {
		result = int32(b[0]-'0')*100 + int32(b[1]-'0')*10 + int32(b[3]-'0')
	}

	return sign * result
}

func bytesToString(b []byte) string {
	return unsafe.String(unsafe.SliceData(b), len(b))
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
