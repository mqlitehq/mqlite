package authkey

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestGenerate(t *testing.T) {
	for _, tc := range []struct {
		prefix string
		size   int
		valid  func(string) bool
		make   func() (string, error)
	}{
		{Prefix, 32, ValidToken, GenerateToken},
		{"", 16, ValidID, GenerateID},
	} {
		// Pin consumption of EVERY entropy byte, including leading zeroes. No
		// permission bit, public ID or timestamp may replace any secret bits.
		raw := make([]byte, tc.size)
		for i := range raw {
			raw[i] = byte(i)
		}
		reader := bytes.NewReader(append(raw, 255))
		got, err := generate(reader, tc.prefix, tc.size)
		want := tc.prefix + "000102030405060708090a0b0c0d0e0f"
		if tc.size == 32 {
			want += "101112131415161718191a1b1c1d1e1f"
		}
		if err != nil || got != want || !tc.valid(got) || reader.Len() != 1 {
			t.Fatalf("entropy/format contract: got=%q err=%v remaining=%d", got, err, reader.Len())
		}
		seen := map[string]bool{}
		for i := 0; i < 100; i++ {
			value, err := tc.make()
			if err != nil || !tc.valid(value) || seen[value] {
				t.Fatal("CSPRNG generation failed, produced an invalid format, or repeated a value")
			}
			seen[value] = true
		}
		for n := 0; n < tc.size; n++ {
			value, err := generate(bytes.NewReader(raw[:n]), tc.prefix, tc.size)
			if err == nil || value != "" {
				t.Fatalf("short entropy input %d produced a credential", n)
			}
		}
		value, err := generate(failedReader{}, tc.prefix, tc.size)
		if value != "" || !errors.Is(err, errEntropy) {
			t.Fatalf("random source error not preserved: %q %v", value, err)
		}
	}
}

var errEntropy = errors.New("entropy unavailable")

type failedReader struct{}

func (failedReader) Read([]byte) (int, error) { return 0, errEntropy }

var _ io.Reader = failedReader{}

func TestFormatExhaustive(t *testing.T) {
	for _, tc := range []struct {
		prefix string
		size   int
		valid  func(string) bool
	}{
		{Prefix, 64, ValidToken},
		{"", 32, ValidID},
	} {
		base := tc.prefix + strings.Repeat("a", tc.size)
		for i := range base {
			for b := 0; b <= 255; b++ {
				candidate := []byte(base)
				candidate[i] = byte(b)
				want := i < len(tc.prefix) && byte(b) == base[i] || i >= len(tc.prefix) && (b >= '0' && b <= '9' || b >= 'a' && b <= 'f')
				if got := tc.valid(string(candidate)); got != want {
					t.Fatalf("position=%d byte=%d valid=%v want=%v", i, b, got, want)
				}
			}
		}
		for size := 0; size <= tc.size+8; size++ {
			if got := tc.valid(tc.prefix + strings.Repeat("0", size)); got != (size == tc.size) {
				t.Fatalf("unexpected acceptance for payload length %d", size)
			}
		}
	}
}
