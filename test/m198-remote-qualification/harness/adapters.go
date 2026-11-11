package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"

	native "github.com/mqlitehq/mqlite"
	sdk "github.com/mqlitehq/mqlite-go"
)

type delivery struct {
	Queue, ID string
	Sequence  int64
	Body      []byte
	Count     int
	Metadata  messageMetadata
	native    *native.Message
	sdk       *sdk.Message
}

func nativeDelivery(queue string, message *native.Message) delivery {
	return delivery{Queue: queue, ID: message.MessageID, Sequence: message.SequenceNumber, Body: message.Body, Count: message.DeliveryCount, Metadata: messageMetadata{GroupID: message.GroupID, Subject: message.Subject, ContentType: message.ContentType, Properties: message.Properties}, native: message}
}
func sdkDelivery(queue string, message *sdk.Message) delivery {
	return delivery{Queue: queue, ID: message.MessageID, Sequence: message.SequenceNumber, Body: message.Body, Count: message.DeliveryCount, Metadata: messageMetadata{GroupID: message.GroupID, Subject: message.Subject, ContentType: message.ContentType, Properties: message.Properties}, sdk: message}
}

type rpcEvent struct {
	Operation  string `json:"operation"`
	Queue      string `json:"queue"`
	Sequence   int64  `json:"sequence"`
	HTTPStatus int    `json:"http_status"`
	Confirmed  bool   `json:"confirmed"`
}

type rpcRecorder struct {
	mu      sync.Mutex
	allowed map[string]bool
	events  []rpcEvent
	calls   map[string]map[string]int
}

func (r *rpcRecorder) count(queue, operation string, seq int64) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := 0
	for _, event := range r.events {
		if event.Queue == queue && event.Operation == operation && event.Sequence == seq && event.Confirmed {
			n++
		}
	}
	return n
}

type recordingTransport struct {
	base     http.RoundTripper
	recorder *rpcRecorder
}

func (t recordingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	method := request.URL.Path[strings.LastIndex(request.URL.Path, "/")+1:]
	if method != "Renew" && method != "RenewBatch" && method != "Complete" && method != "CompleteBatch" && method != "Send" && method != "Receive" {
		return t.base.RoundTrip(request)
	}
	raw, err := io.ReadAll(io.LimitReader(request.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	_ = request.Body.Close()
	request.Body = io.NopCloser(bytes.NewReader(raw))
	var args struct {
		Queue    string `json:"queue"`
		Sequence int64  `json:"seq_number"`
		Messages []struct {
			Sequence int64 `json:"seq_number"`
		} `json:"messages"`
	}
	if err := json.Unmarshal(raw, &args); err != nil {
		return nil, err
	}
	if !t.recorder.allowed[args.Queue] {
		return nil, errors.New("request outside owned resource manifest")
	}
	t.recorder.mu.Lock()
	if t.recorder.calls == nil {
		t.recorder.calls = map[string]map[string]int{}
	}
	if t.recorder.calls[args.Queue] == nil {
		t.recorder.calls[args.Queue] = map[string]int{}
	}
	t.recorder.calls[args.Queue][method]++
	t.recorder.mu.Unlock()
	response, err := t.base.RoundTrip(request)
	if method == "Send" || method == "Receive" {
		return response, err
	}
	if err != nil {
		return nil, err
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	_ = response.Body.Close()
	if err != nil {
		return nil, err
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	var ack struct {
		OK      bool `json:"ok"`
		Results []struct {
			Sequence int64 `json:"seq_number"`
			OK       bool  `json:"ok"`
		} `json:"results"`
	}
	if response.StatusCode == http.StatusOK {
		_ = json.Unmarshal(body, &ack)
	}
	var events []rpcEvent
	if args.Sequence != 0 {
		events = append(events, rpcEvent{method, args.Queue, args.Sequence, response.StatusCode, response.StatusCode == http.StatusOK && ack.OK})
	} else {
		for _, message := range args.Messages {
			ok := false
			for _, result := range ack.Results {
				if result.Sequence == message.Sequence && result.OK {
					ok = true
				}
			}
			events = append(events, rpcEvent{method, args.Queue, message.Sequence, response.StatusCode, response.StatusCode == http.StatusOK && ok})
		}
	}
	t.recorder.mu.Lock()
	t.recorder.events = append(t.recorder.events, events...)
	t.recorder.mu.Unlock()
	return response, nil
}

type adapter struct {
	kind        string
	native      *native.Client
	sdk         *sdk.Client
	embedded    *native.Embedded
	transport   *http.Transport
	runOverride func(context.Context, string, func(context.Context, delivery) error, func(error)) error
}

func newAdapter(ctx context.Context, kind, endpoint, token string, recorder *rpcRecorder) (*adapter, error) {
	base := &http.Transport{MaxIdleConns: 8, MaxIdleConnsPerHost: 8}
	client := &http.Client{Transport: recordingTransport{base, recorder}}
	a := &adapter{kind: kind, transport: base}
	var err error
	if kind == "native" {
		a.native, err = native.Open(ctx, endpoint, native.WithToken(token), native.WithHTTPClient(client))
	} else {
		a.sdk, err = sdk.Open(ctx, endpoint, sdk.WithToken(token), sdk.WithHTTPClient(client))
	}
	return a, err
}

func (a *adapter) close() {
	if a.native != nil {
		_ = a.native.Close()
	}
	if a.sdk != nil {
		_ = a.sdk.Close()
	}
	if a.transport != nil {
		a.transport.CloseIdleConnections()
	}
}

func (a *adapter) send(ctx context.Context, queue string, items []entry) ([]int64, error) {
	if a.sdk != nil {
		messages := make([]sdk.OutMessage, len(items))
		for i, item := range items {
			messages[i] = sdk.OutMessage{Body: item.Body, MessageID: item.ID, GroupID: item.Metadata.GroupID, Subject: item.Metadata.Subject, ContentType: item.Metadata.ContentType, Properties: item.Metadata.Properties}
		}
		return a.sdk.Send(ctx, queue, messages...)
	}
	messages := make([]native.OutMessage, len(items))
	for i, item := range items {
		messages[i] = native.OutMessage{Body: item.Body, MessageID: item.ID, GroupID: item.Metadata.GroupID, Subject: item.Metadata.Subject, ContentType: item.Metadata.ContentType, Properties: item.Metadata.Properties}
	}
	if a.embedded != nil {
		return a.embedded.Send(ctx, queue, messages...)
	}
	return a.native.Send(ctx, queue, messages...)
}

func (a *adapter) receive(ctx context.Context, queue string, count int) ([]delivery, error) {
	if a.sdk != nil {
		messages, err := a.sdk.Receive(ctx, queue, sdk.RecvOpts{Max: count})
		if err != nil {
			return nil, err
		}
		out := make([]delivery, len(messages))
		for i, message := range messages {
			out[i] = sdkDelivery(queue, message)
		}
		return out, nil
	}
	var messages []*native.Message
	var err error
	if a.embedded != nil {
		messages, err = a.embedded.Receive(ctx, queue, native.RecvOpts{Max: count})
	} else {
		messages, err = a.native.Receive(ctx, queue, native.RecvOpts{Max: count})
	}
	if err != nil {
		return nil, err
	}
	out := make([]delivery, len(messages))
	for i, message := range messages {
		out[i] = nativeDelivery(queue, message)
	}
	return out, nil
}

func (a *adapter) settle(ctx context.Context, queue string, messages []delivery, renew bool) error {
	if a.sdk != nil {
		items := make([]*sdk.Message, len(messages))
		for i, message := range messages {
			items[i] = message.sdk
		}
		var results []sdk.SettleResult
		var err error
		if renew {
			results, err = a.sdk.RenewBatch(ctx, queue, items...)
		} else {
			results, err = a.sdk.CompleteBatch(ctx, queue, items...)
		}
		if err != nil {
			return err
		}
		if len(results) != len(items) {
			return errors.New("settlement result count mismatch")
		}
		for i, result := range results {
			if !result.Ok || result.SequenceNumber != messages[i].Sequence {
				return errors.New("settlement item was not confirmed")
			}
		}
		return nil
	}
	items := make([]*native.Message, len(messages))
	for i, message := range messages {
		items[i] = message.native
	}
	var results []native.SettleResult
	var err error
	if a.embedded != nil {
		if renew {
			results, err = a.embedded.RenewBatch(ctx, queue, items...)
		} else {
			results, err = a.embedded.CompleteBatch(ctx, queue, items...)
		}
	} else {
		if renew {
			results, err = a.native.RenewBatch(ctx, queue, items...)
		} else {
			results, err = a.native.CompleteBatch(ctx, queue, items...)
		}
	}
	if err != nil {
		return err
	}
	if len(results) != len(items) {
		return errors.New("settlement result count mismatch")
	}
	for i, result := range results {
		if !result.Ok || result.SequenceNumber != messages[i].Sequence {
			return errors.New("settlement item was not confirmed")
		}
	}
	return nil
}

func (a *adapter) run(ctx context.Context, queue string, handler func(context.Context, delivery) error, onError func(error)) error {
	if a.runOverride != nil {
		return a.runOverride(ctx, queue, handler, onError)
	}
	if a.sdk != nil {
		return a.sdk.Receiver(queue, sdk.WithAutoRenew(), sdk.WithConcurrency(1), sdk.WithPrefetch(1), sdk.WithErrorHandler(onError)).Run(ctx, func(ctx context.Context, m *sdk.Message) error { return handler(ctx, sdkDelivery(queue, m)) })
	}
	return a.native.Receiver(queue, native.WithAutoRenew(), native.WithConcurrency(1), native.WithPrefetch(1), native.WithErrorHandler(onError)).Run(ctx, func(ctx context.Context, m *native.Message) error { return handler(ctx, nativeDelivery(queue, m)) })
}
