package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"unsafe"

	"golang.org/x/sys/unix"
)

type cityMeasurement struct {
	name  string
	min   float64
	max   float64
	total float64
	count float64
}

const (
	fnv1aOffset uint64 = 0xcbf29ce484222325
	fnv1aPrime  uint64 = 0x100000001b3
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
		cities = append(cities, m.name)
	}

	sort.Strings(cities)

	for _, city := range cities {
		m := measurements[stringHash(city)]

		result = append(result, fmt.Sprintf("%s=%.1f/%.1f/%.1f", city, m.min, round(m.total/m.count), m.max))
	}

	return "{" + strings.Join(result, ", ") + "}\n", nil
}

func getMeasurements(data []byte) (map[uint64]cityMeasurement, error) {
	result := make(map[uint64]cityMeasurement, 10000)

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

		m, ok := result[hash]
		if !ok {
			result[hash] = cityMeasurement{
				name:  city,
				min:   value,
				max:   value,
				total: value,
				count: 1,
			}
			calculateHash = true
			hash = fnv1aOffset

			continue
		}

		m.min = min(m.min, value)
		m.max = max(m.max, value)
		m.total += value
		m.count++

		result[hash] = m

		calculateHash = true
		hash = fnv1aOffset
	}

	return result, nil
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
