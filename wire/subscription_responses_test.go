package wire_test

import (
	"strings"
	"testing"

	"github.com/mqlitehq/mqlite/wire"
)

func TestDecodeListSubscriptionsPageResponse(t *testing.T) {
	request := wire.ListSubscriptionsPageRequest{Topic: "events", Limit: 2, IncludeDeleting: true}
	page, err := wire.DecodeListSubscriptionsPageResponse([]byte(`{
		"subscriptions":[
			{"topic":"events","name":"a","expr":"true","state":"active","created_at_ms":10,"expires_at_ms":20,"future":1},
			{"topic":"events","name":"b","state":"deleting","created_at_ms":10,"expires_at_ms":0}
		],
		"total":3,
		"next_after":"cursor",
		"future":true
	}`), request)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Subscriptions) != 2 || page.Subscriptions[0].Name != "a" ||
		page.Subscriptions[0].CreatedAtMs != 10 || page.Subscriptions[0].ExpiresAtMs != 20 ||
		page.Subscriptions[1].State != "deleting" || page.Total != 3 || page.NextAfter != "cursor" {
		t.Fatalf("decoded page = %+v", page)
	}

	empty, err := wire.DecodeListSubscriptionsPageResponse(
		[]byte(`{"subscriptions":[],"total":0}`),
		wire.ListSubscriptionsPageRequest{},
	)
	if err != nil || empty.Subscriptions == nil || len(empty.Subscriptions) != 0 || empty.Total != 0 {
		t.Fatalf("decoded empty page = %+v, %v", empty, err)
	}
}

func TestDecodeListSubscriptionsPageResponseRejectsUnsafePages(t *testing.T) {
	base := wire.ListSubscriptionsPageRequest{Topic: "events", Limit: 2, IncludeDeleting: true}
	for _, tc := range []struct {
		name     string
		response string
		request  wire.ListSubscriptionsPageRequest
	}{
		{"null root", `null`, base},
		{"array root", `[]`, base},
		{"trailing JSON", `{"subscriptions":[],"total":0} {}`, base},
		{"missing subscriptions", `{"total":0}`, base},
		{"null subscriptions", `{"subscriptions":null,"total":0}`, base},
		{"missing total", `{"subscriptions":[]}`, base},
		{"null total", `{"subscriptions":[],"total":null}`, base},
		{"negative total", `{"subscriptions":[],"total":-1}`, base},
		{"total below items", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":0}`, base},
		{"missing topic", `{"subscriptions":[{"name":"a","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":1}`, base},
		{"missing name", `{"subscriptions":[{"topic":"events","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":1}`, base},
		{"missing state", `{"subscriptions":[{"topic":"events","name":"a","created_at_ms":10,"expires_at_ms":0}],"total":1}`, base},
		{"null state", `{"subscriptions":[{"topic":"events","name":"a","state":null,"created_at_ms":10,"expires_at_ms":0}],"total":1}`, base},
		{"null expression", `{"subscriptions":[{"topic":"events","name":"a","expr":null,"state":"active","created_at_ms":10,"expires_at_ms":0}],"total":1}`, base},
		{"unknown state", `{"subscriptions":[{"topic":"events","name":"a","state":"retired","created_at_ms":10,"expires_at_ms":0}],"total":1}`, base},
		{"missing created time", `{"subscriptions":[{"topic":"events","name":"a","state":"active","expires_at_ms":0}],"total":1}`, base},
		{"null created time", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":null,"expires_at_ms":0}],"total":1}`, base},
		{"negative created time", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":-1,"expires_at_ms":0}],"total":1}`, base},
		{"missing expiry", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":10}],"total":1}`, base},
		{"null expiry", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":null}],"total":1}`, base},
		{"negative expiry", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":-1}],"total":1}`, base},
		{"expiry equals creation", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":10}],"total":1}`, base},
		{"expiry before creation", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":9}],"total":1}`, base},
		{"deleting excluded", `{"subscriptions":[{"topic":"events","name":"a","state":"deleting","created_at_ms":10,"expires_at_ms":0}],"total":1}`, wire.ListSubscriptionsPageRequest{Topic: "events", Limit: 2}},
		{"scope mismatch", `{"subscriptions":[{"topic":"other","name":"a","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":1}`, base},
		{"unordered topic", `{"subscriptions":[{"topic":"events","name":"b","state":"active","created_at_ms":10,"expires_at_ms":0},{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":2}`, base},
		{"duplicate item", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":0},{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":2}`, base},
		{"duplicate global name", `{"subscriptions":[{"topic":"a","name":"same","state":"active","created_at_ms":10,"expires_at_ms":0},{"topic":"b","name":"same","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":2}`, wire.ListSubscriptionsPageRequest{Limit: 2, IncludeDeleting: true}},
		{"too many items", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":0},{"topic":"events","name":"b","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":2}`, wire.ListSubscriptionsPageRequest{Topic: "events", Limit: 1, IncludeDeleting: true}},
		{"short first page", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":2}`, base},
		{"missing first-page cursor", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":0},{"topic":"events","name":"b","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":3}`, base},
		{"unexpected first-page cursor", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":1,"next_after":"cursor"}`, wire.ListSubscriptionsPageRequest{Topic: "events", Limit: 1, IncludeDeleting: true}},
		{"short continued page with cursor", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":3,"next_after":"new"}`, wire.ListSubscriptionsPageRequest{Topic: "events", After: "old", Limit: 2, IncludeDeleting: true}},
		{"cursor made no progress", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":0},{"topic":"events","name":"b","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":3,"next_after":"old"}`, wire.ListSubscriptionsPageRequest{Topic: "events", After: "old", Limit: 2, IncludeDeleting: true}},
		{"null cursor", `{"subscriptions":[],"total":0,"next_after":null}`, base},
		{"invalid limit", `{"subscriptions":[],"total":0}`, wire.ListSubscriptionsPageRequest{Limit: 1001}},
		{"oversized request cursor", `{"subscriptions":[],"total":0}`, wire.ListSubscriptionsPageRequest{After: strings.Repeat("x", 1<<20+1)}},
		{"oversized response cursor", `{"subscriptions":[{"topic":"events","name":"a","state":"active","created_at_ms":10,"expires_at_ms":0}],"total":2,"next_after":"` + strings.Repeat("x", 1<<20+1) + `"}`, wire.ListSubscriptionsPageRequest{Topic: "events", Limit: 1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := wire.DecodeListSubscriptionsPageResponse([]byte(tc.response), tc.request); err == nil {
				t.Fatal("accepted malformed subscription page")
			}
		})
	}
}

func TestReadListSubscriptionsPageResponseBound(t *testing.T) {
	if _, err := wire.ReadListSubscriptionsPageResponse(strings.NewReader(`{"subscriptions":[],"total":0}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := wire.ReadListSubscriptionsPageResponse(
		strings.NewReader(strings.Repeat("x", wire.MaxListSubscriptionsPageResponseBytes+1)),
	); err == nil {
		t.Fatal("accepted oversized subscription inventory response")
	}
	invalidUTF8 := []byte("{\"subscriptions\":[{\"topic\":\"events\",\"name\":\"")
	invalidUTF8 = append(invalidUTF8, 0xff)
	invalidUTF8 = append(invalidUTF8, []byte("\",\"state\":\"active\"}],\"total\":1}")...)
	if _, err := wire.DecodeListSubscriptionsPageResponse(invalidUTF8, wire.ListSubscriptionsPageRequest{}); err == nil {
		t.Fatal("accepted invalid UTF-8 in subscription inventory")
	}
}
