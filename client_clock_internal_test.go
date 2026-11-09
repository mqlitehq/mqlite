package mqlite

import (
	"net/http"
	"strconv"
	"testing"
	"time"

	"github.com/mqlitehq/mqlite/wire"
)

func TestServerElapsedMapsOnlyNetworkUncertainty(t *testing.T) {
	for _, tc := range []struct {
		name         string
		offset       time.Duration
		date         bool
		invalidExact bool
	}{
		{name: "broker behind", offset: -35 * time.Second},
		{name: "broker ahead", offset: 35 * time.Second},
		{name: "Date fallback", offset: -35 * time.Second, date: true},
		{name: "invalid exact clock uses Date", offset: 35 * time.Second, date: true, invalidExact: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			received := time.Now()
			server := received.Add(tc.offset).Truncate(time.Millisecond)
			resolution := time.Millisecond
			header := make(http.Header)
			if tc.date {
				server = server.Truncate(time.Second)
				resolution = time.Second
				header.Set("Date", server.UTC().Format(http.TimeFormat))
				if tc.invalidExact {
					header.Set(wire.ServerTimeHeader, "invalid")
				}
			} else {
				header.Set(wire.ServerTimeHeader, strconv.FormatInt(server.UnixNano()/int64(time.Millisecond), 10))
			}
			header.Set("X-Mqlite-Server-Elapsed-Ms", "20000")
			const total = 20400 * time.Millisecond
			observation := observeServerTime(header, received.Add(-total), received)
			// The broker timestamp is sampled after its twenty-second wait.
			// Charge the entire remaining 400ms as network uncertainty; do not
			// charge that wait a second time or assume a symmetric response leg.
			wantUpper := server.Add(resolution).Add(400 * time.Millisecond)
			if !observation.serverUpper.Equal(wantUpper) {
				t.Fatalf("server upper bound = %v, want %v", observation.serverUpper, wantUpper)
			}
			if observation.responseRTT != total {
				t.Fatal("server elapsed changed the full RPC budget used for renewal")
			}
			deadline, ok := observation.localLeaseDeadline(server.Add(3 * time.Second))
			wantDeadline := received.Add(3*time.Second - resolution - 400*time.Millisecond)
			if !ok || deadline.Sub(wantDeadline) > time.Millisecond || wantDeadline.Sub(deadline) > time.Millisecond {
				t.Fatalf("mapped lease = %v, %v, want %v", deadline, ok, wantDeadline)
			}
		})
	}
}

func TestServerElapsedHeaderValidationKeepsUnprovenTimeCharged(t *testing.T) {
	const maxDuration = time.Duration(1<<63 - 1)
	const ordinaryRTT = 1000*time.Millisecond + 500*time.Microsecond
	for _, tc := range []struct {
		name     string
		values   []string
		rtt      time.Duration
		credited time.Duration
	}{
		{name: "missing", rtt: ordinaryRTT},
		{name: "zero", values: []string{"0"}, rtt: ordinaryRTT},
		{name: "valid", values: []string{"700"}, rtt: ordinaryRTT, credited: 700 * time.Millisecond},
		{name: "leading zeroes", values: []string{"000700"}, rtt: ordinaryRTT, credited: 700 * time.Millisecond},
		{name: "floor keeps submillisecond network budget", values: []string{"1000"}, rtt: ordinaryRTT, credited: time.Second},
		{name: "larger than measured RTT", values: []string{"1001"}, rtt: ordinaryRTT},
		{name: "empty", values: []string{""}, rtt: ordinaryRTT},
		{name: "negative", values: []string{"-1"}, rtt: ordinaryRTT},
		{name: "negative zero", values: []string{"-0"}, rtt: ordinaryRTT},
		{name: "explicit plus", values: []string{"+1"}, rtt: ordinaryRTT},
		{name: "leading space", values: []string{" 1"}, rtt: ordinaryRTT},
		{name: "trailing space", values: []string{"1 "}, rtt: ordinaryRTT},
		{name: "fractional", values: []string{"1.0"}, rtt: ordinaryRTT},
		{name: "exponent", values: []string{"1e3"}, rtt: ordinaryRTT},
		{name: "joined values", values: []string{"1, 2"}, rtt: ordinaryRTT},
		{name: "duplicate values", values: []string{"1", "1"}, rtt: ordinaryRTT},
		{name: "non ASCII digits", values: []string{"\u0661"}, rtt: ordinaryRTT},
		{name: "duration conversion overflow", values: []string{"9223372036854775807"}, rtt: ordinaryRTT},
		{name: "unsigned parse overflow", values: []string{"18446744073709551616"}, rtt: ordinaryRTT},
		{name: "maximum valid duration", values: []string{"9223372036854"}, rtt: maxDuration, credited: 9223372036854 * time.Millisecond},
		{name: "overflowing duration beyond maximum RTT", values: []string{"9223372036855"}, rtt: maxDuration},
		{name: "less than one millisecond", values: []string{"1"}, rtt: 500 * time.Microsecond},
		{name: "negative observed RTT", values: []string{"1"}, rtt: -time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			received := time.Now()
			server := received.Add(-35 * time.Second).Truncate(time.Millisecond)
			header := make(http.Header)
			header.Set(wire.ServerTimeHeader, strconv.FormatInt(server.UnixNano()/int64(time.Millisecond), 10))
			if tc.values != nil {
				header["X-Mqlite-Server-Elapsed-Ms"] = tc.values
			}
			observation := observeServerTime(header, received.Add(-tc.rtt), received)
			rtt := tc.rtt
			if rtt < 0 {
				rtt = 0
			}
			want := server.Add(time.Millisecond).Add(rtt - tc.credited)
			if !observation.serverUpper.Equal(want) || observation.responseRTT != rtt {
				t.Fatalf("upper=%v RTT=%v, want upper=%v full RTT=%v", observation.serverUpper, observation.responseRTT, want, rtt)
			}
		})
	}
}

func TestServerElapsedNeverCreatesMissingClockOrContaminatesNextResponse(t *testing.T) {
	received := time.Now()
	header := make(http.Header)
	header.Set("X-Mqlite-Server-Elapsed-Ms", "20000")
	missing := observeServerTime(header, received.Add(-20*time.Second), received)
	if !missing.serverUpper.IsZero() || missing.responseRTT != 20*time.Second {
		t.Fatal("elapsed time fabricated a broker clock")
	}
	if _, ok := missing.localLeaseDeadline(received.Add(time.Minute)); ok {
		t.Fatal("a response without a broker clock mapped an absolute lease")
	}
	server := received.Add(-35 * time.Second).Truncate(time.Millisecond)
	header.Set(wire.ServerTimeHeader, strconv.FormatInt(server.UnixNano()/int64(time.Millisecond), 10))
	long := observeServerTime(header, received.Add(-20*time.Second), received)
	if !long.serverUpper.Equal(server.Add(time.Millisecond)) {
		t.Fatal("long request did not discount its own elapsed time")
	}
	// An older broker (or stripped elapsed header) keeps the full RTT, even
	// immediately after an otherwise identical response supplied elapsed time.
	header.Del("X-Mqlite-Server-Elapsed-Ms")
	next := observeServerTime(header, received.Add(-20*time.Millisecond), received)
	if !next.serverUpper.Equal(server.Add(21*time.Millisecond)) || next.responseRTT != 20*time.Millisecond {
		t.Fatal("elapsed observation escaped the response that supplied it")
	}
}
