package main

import (
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func Test_process(t *testing.T) {
	files, err := getMeasurementFiles()
	if err != nil {
		t.Fatalf("reading test data directory error: %s", err)
	}

	for _, testCase := range files {
		t.Run(testCase, func(t *testing.T) {
			got, err := process(testCase)
			if err != nil {
				t.Errorf("unexpected error: %s", err)
			}

			expected, err := getExpectedResult(strings.ReplaceAll(testCase, ".txt", ".out"))
			if err != nil {
				t.Fatalf("unable to read *.out file: %s", err)
			}

			if expected != got {
				t.Errorf("process() = %v, want %v", got, expected)
			}
		})
	}
}

func getMeasurementFiles() ([]string, error) {
	result := make([]string, 0, 12)
	err := filepath.WalkDir("./test", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		if filepath.Ext(d.Name()) == ".txt" {
			result = append(result, path)
		}

		return nil
	})

	return result, err
}

func getExpectedResult(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func Test_round(t *testing.T) {
	cases := []struct {
		name     string
		v        float64
		expected int32
	}{
		{
			name:     "254.49",
			v:        254.49,
			expected: 254,
		},
		{
			name:     "-999.000015258789",
			v:        -999.000015258789,
			expected: -999,
		},
		{
			name:     "999.000015258789",
			v:        999.000015258789,
			expected: 999,
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			got := round(tt.v)
			if got != tt.expected {
				t.Errorf("round() = %v, want %v", got, tt.expected)
			}
		})
	}
}

var sink string

func Benchmark_process(b *testing.B) {
	b.ReportAllocs()

	var (
		result string
		err    error
	)
	for i := 0; i < b.N; i++ {
		result, err = process("./measurements_1M.txt")
		if err != nil {
			b.Fatalf("benchmark error: %s", err)
		}
	}
	sink = result
}
