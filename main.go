package main

import (
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strings"

	"golang.org/x/sys/unix"
)

type cityMeasurement struct {
	min   float64
	max   float64
	total float64
	count float64
}

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
	measurements, err := getMeasurements(filePath)
	if err != nil {
		return "", fmt.Errorf("unable to receive measurements: %w", err)
	}

	result := make([]string, 0, len(measurements))

	cities := make([]string, 0, len(measurements))

	for city := range measurements {
		cities = append(cities, city)
	}

	sort.Strings(cities)

	for _, city := range cities {
		m := measurements[city]

		result = append(result, fmt.Sprintf("%s=%.1f/%.1f/%.1f", city, m.min, round(m.total/m.count), m.max))
	}

	return "{" + strings.Join(result, ", ") + "}\n", nil
}

func getMeasurements(filePath string) (map[string]cityMeasurement, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("could not open measurements file: %w", err)
	}
	defer f.Close()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("file stat error: %w", err)
	}

	data, err := unix.Mmap(int(f.Fd()), 0, int(stat.Size()), unix.PROT_READ, unix.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("mmap file error: %w", err)
	}
	defer unix.Munmap(data)

	result := make(map[string]cityMeasurement, 10000)

	start := 0
	semicolumnPos := 0
	for i, v := range data {
		if v == ';' {
			semicolumnPos = i
			continue
		}

		if v != '\n' {
			continue
		}

		city := string(data[start:semicolumnPos])

		v := parseFloat64(data[semicolumnPos+1 : i])

		start = i + 1

		m, ok := result[city]
		if !ok {
			result[city] = cityMeasurement{
				min:   v,
				max:   v,
				total: v,
				count: 1,
			}

			continue
		}

		m.min = min(result[city].min, v)
		m.max = max(result[city].max, v)
		m.total += v
		m.count++

		result[city] = m
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
