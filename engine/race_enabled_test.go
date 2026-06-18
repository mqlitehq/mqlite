//go:build race

package engine

// raceEnabled is true when the test binary is built with -race. modernc.org/sqlite
// is pure Go, so the race detector instruments the entire SQLite engine and makes
// large-seed timing tests both very slow and meaningless — they gate on it.
const raceEnabled = true
