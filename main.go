package main

import (
	"bufio"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
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

	result := make(map[string]cityMeasurement, 10000)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		txt := scanner.Text()

		parts := strings.Split(txt, ";")

		v, _ := strconv.ParseFloat(parts[1], 32)

		m, ok := result[parts[0]]
		if !ok {
			result[parts[0]] = cityMeasurement{
				min:   v,
				max:   v,
				total: v,
				count: 1,
			}

			continue
		}

		m.min = min(result[parts[0]].min, v)
		m.max = max(result[parts[0]].max, v)
		m.total += v
		m.count++

		result[parts[0]] = m
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
