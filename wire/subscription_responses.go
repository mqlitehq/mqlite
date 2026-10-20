package wire

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"unicode/utf8"
)

// An inventory response can contain 1,000 filters of up to 4 KiB each. Leave
// room for JSON escaping while still bounding an untrusted broker or proxy.
const MaxListSubscriptionsPageResponseBytes = 32 << 20

const maxSubscriptionCursorBytes = 1 << 20

var errListSubscriptionsPageResponse = errors.New("mqlite: invalid subscription inventory response")

// ReadListSubscriptionsPageResponse reads one complete, bounded inventory page.
// DecodeListSubscriptionsPageResponse performs the semantic validation.
func ReadListSubscriptionsPageResponse(reader io.Reader) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(reader, MaxListSubscriptionsPageResponseBytes+1))
	if err != nil || len(data) > MaxListSubscriptionsPageResponseBytes {
		return nil, errListSubscriptionsPageResponse
	}
	return data, nil
}

func subscriptionResponseObject(data []byte, required ...string) (map[string]json.RawMessage, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields == nil {
		return nil, errListSubscriptionsPageResponse
	}
	for _, name := range required {
		value, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, errListSubscriptionsPageResponse
		}
	}
	return fields, nil
}

// DecodeListSubscriptionsPageResponse validates a complete page against the
// request that produced it. Unknown fields remain forward compatible, while the
// current fields must be present, bounded, scoped and keyset ordered.
func DecodeListSubscriptionsPageResponse(data []byte, request ListSubscriptionsPageRequest) (ListSubscriptionsPageResponse, error) {
	if len(data) > MaxListSubscriptionsPageResponseBytes || !utf8.Valid(data) || len(request.After) > maxSubscriptionCursorBytes {
		return ListSubscriptionsPageResponse{}, errListSubscriptionsPageResponse
	}
	limit := request.Limit
	if limit == 0 {
		limit = 100
	}
	if limit < 1 || limit > 1000 {
		return ListSubscriptionsPageResponse{}, errListSubscriptionsPageResponse
	}

	fields, err := subscriptionResponseObject(data, "subscriptions", "total")
	if err != nil {
		return ListSubscriptionsPageResponse{}, err
	}
	var rawSubscriptions []json.RawMessage
	var total int
	if json.Unmarshal(fields["subscriptions"], &rawSubscriptions) != nil || rawSubscriptions == nil ||
		json.Unmarshal(fields["total"], &total) != nil || total < 0 || total < len(rawSubscriptions) ||
		len(rawSubscriptions) > limit {
		return ListSubscriptionsPageResponse{}, errListSubscriptionsPageResponse
	}

	page := ListSubscriptionsPageResponse{
		Subscriptions: make([]SubscriptionPageJSON, len(rawSubscriptions)),
		Total:         total,
	}
	var previousTopic, previousName string
	seenNames := make(map[string]bool, len(rawSubscriptions))
	for i, raw := range rawSubscriptions {
		itemFields, err := subscriptionResponseObject(raw, "topic", "name", "state", "created_at_ms", "expires_at_ms")
		if err != nil {
			return ListSubscriptionsPageResponse{}, err
		}
		if expr, ok := itemFields["expr"]; ok && bytes.Equal(bytes.TrimSpace(expr), []byte("null")) {
			return ListSubscriptionsPageResponse{}, errListSubscriptionsPageResponse
		}
		var item SubscriptionPageJSON
		if json.Unmarshal(raw, &item) != nil || item.Topic == "" || item.Name == "" ||
			item.State != "active" && item.State != "deleting" ||
			item.CreatedAtMs < 0 || item.ExpiresAtMs < 0 ||
			item.ExpiresAtMs != 0 && item.ExpiresAtMs <= item.CreatedAtMs ||
			!request.IncludeDeleting && item.State != "active" ||
			request.Topic != "" && item.Topic != request.Topic ||
			seenNames[item.Name] ||
			i > 0 && (item.Topic < previousTopic || item.Topic == previousTopic && item.Name <= previousName) {
			return ListSubscriptionsPageResponse{}, errListSubscriptionsPageResponse
		}
		page.Subscriptions[i] = item
		seenNames[item.Name] = true
		previousTopic, previousName = item.Topic, item.Name
	}

	if next, ok := fields["next_after"]; ok {
		if bytes.Equal(bytes.TrimSpace(next), []byte("null")) || json.Unmarshal(next, &page.NextAfter) != nil {
			return ListSubscriptionsPageResponse{}, errListSubscriptionsPageResponse
		}
	}
	if page.NextAfter != "" && (len(page.NextAfter) > maxSubscriptionCursorBytes ||
		len(page.Subscriptions) != limit || page.NextAfter == request.After) {
		return ListSubscriptionsPageResponse{}, errListSubscriptionsPageResponse
	}
	if request.After == "" {
		expected := total
		if expected > limit {
			expected = limit
		}
		if len(page.Subscriptions) != expected || (page.NextAfter != "") != (total > limit) {
			return ListSubscriptionsPageResponse{}, errListSubscriptionsPageResponse
		}
	}
	return page, nil
}
