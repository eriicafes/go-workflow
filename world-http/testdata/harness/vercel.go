package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"mime/multipart"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	vercelworld "github.com/eriicafes/go-workflow/worlds/vercel"
)

// vercelLease is how long a delivery's message stays invisible, as long as
// Vercel Queues leases one for.
const vercelLease = 5 * time.Minute

// openVercel returns the Vercel World on Vercel's workflow backend, from the
// variables the Workflow SDK's tests use: WORKFLOW_VERCEL_AUTH_TOKEN, a Vercel
// access token, and WORKFLOW_VERCEL_PROJECT and WORKFLOW_VERCEL_TEAM, the
// project and team to use. The World reads WORKFLOW_VERCEL_BACKEND_URL itself.
// Vercel Queues, which push deliveries to a deployment, are played by a local
// queue that pushes them to the World's queue handler.
func openVercel(ctx context.Context, baseURL string, executor worlds.Executor) (worlds.World, func(), error) {
	token := os.Getenv("WORKFLOW_VERCEL_AUTH_TOKEN")
	if token == "" {
		return nil, nil, errors.New("harness: set WORKFLOW_VERCEL_AUTH_TOKEN for the Vercel World")
	}
	queue := &localQueue{}
	server := httptest.NewServer(queue)
	config := vercelworld.Config{
		BaseURL:   baseURL,
		QueueURL:  server.URL + "/api/v3",
		Topic:     "workflow",
		Token:     token,
		ProjectID: os.Getenv("WORKFLOW_VERCEL_PROJECT"),
		TeamID:    os.Getenv("WORKFLOW_VERCEL_TEAM"),
	}
	world, err := vercelworld.New(config)
	if err != nil {
		server.Close()
		return nil, nil, err
	}
	delivering, stop := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		queue.deliver(delivering, vercelworld.Queue(world, executor))
	}()
	return world, func() {
		stop()
		<-done
		server.Close()
	}, nil
}

// localQueue plays the Vercel Queues API: it stores published messages, one
// per idempotency key, leases each visible one, and pushes it to a consumer
// as Vercel pushes a delivery to a queue-triggered function. A message with a
// concurrency key that becomes visible while another message of the key is
// being pushed is coalesced into that delivery, whose acknowledgment must
// then cover it.
type localQueue struct {
	mu       sync.Mutex
	messages []*localMessage
	sequence int
}

// localMessage is a message localQueue stores.
type localMessage struct {
	id, topic, idempotencyKey, concurrencyKey string
	body                                      []byte
	visibleAt                                 time.Time
	deliveries, handoffs, coalesced           int
	receipt                                   string
	pushing, done                             bool
}

func (q *localQueue) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	body, _ := io.ReadAll(request.Body)
	q.mu.Lock()
	defer q.mu.Unlock()
	path := request.URL.Path
	switch {
	case request.Method == http.MethodPost && strings.HasPrefix(path, "/api/v3/topic/") && strings.HasSuffix(path, "/batch"):
		// A batch send: a multipart/mixed body with a part per message, whose
		// headers are the message's, answered with a result per part.
		_, params, err := mime.ParseMediaType(request.Header.Get("Content-Type"))
		if err != nil || params["boundary"] == "" {
			writer.WriteHeader(http.StatusBadRequest)
			return
		}
		topic := strings.TrimSuffix(strings.TrimPrefix(path, "/api/v3/topic/"), "/batch")
		results := []map[string]any{}
		reader := multipart.NewReader(bytes.NewReader(body), params["boundary"])
		for {
			part, err := reader.NextRawPart()
			if err != nil {
				break
			}
			data, _ := io.ReadAll(part)
			results = append(results, map[string]any{"status": "sent", "messageId": q.publish(topic, http.Header(part.Header), data)})
		}
		writer.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(writer).Encode(map[string]any{"results": results})
	case request.Method == http.MethodPost && strings.HasPrefix(path, "/api/v3/topic/") && !strings.Contains(path, "/consumer/"):
		writer.WriteHeader(http.StatusCreated)
		_, _ = fmt.Fprintf(writer, `{"messageId":%q}`, q.publish(strings.TrimPrefix(path, "/api/v3/topic/"), request.Header, body))
	case strings.Contains(path, "/lease/"):
		message := q.leased(path[strings.LastIndex(path, "/")+1:])
		if message == nil {
			writer.WriteHeader(http.StatusNotFound)
			return
		}
		if request.Method == http.MethodDelete {
			if covered, _ := strconv.Atoi(request.Header.Get("Vqs-Experimental-If-Coalesced")); message.coalesced > covered {
				writer.Header().Set("Vqs-Error-Code", "concurrency_coalesced_pending")
				writer.Header().Set("Vqs-Experimental-Coalesced", strconv.Itoa(message.coalesced))
				writer.WriteHeader(http.StatusConflict)
				return
			}
			message.done = true
			writer.WriteHeader(http.StatusNoContent)
			return
		}
		var visibility struct {
			VisibilityTimeoutSeconds int64 `json:"visibilityTimeoutSeconds"`
		}
		_ = json.Unmarshal(body, &visibility)
		message.visibleAt = time.Now().Add(time.Duration(visibility.VisibilityTimeoutSeconds) * time.Second)
		if request.Header.Get("Vqs-Experimental-Handoff") == "1" && visibility.VisibilityTimeoutSeconds == 0 {
			message.handoffs++
			writer.Header().Set("Vqs-Experimental-Handoff", "accepted")
		}
		writer.WriteHeader(http.StatusOK)
		_, _ = writer.Write([]byte(`{"success":true}`))
	default:
		writer.WriteHeader(http.StatusNotFound)
	}
}

// deliver pushes each message to consumer as it becomes visible, until ctx
// is done.
func (q *localQueue) deliver(ctx context.Context, consumer http.Handler) {
	var pushes sync.WaitGroup
	defer pushes.Wait()
	for ctx.Err() == nil {
		message, request := q.next()
		if message == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		pushes.Go(func() {
			defer q.pushed(message)
			consumer.ServeHTTP(httptest.NewRecorder(), request.WithContext(ctx))
		})
	}
}

// publish stores a message, one per idempotency key, and returns its ID.
func (q *localQueue) publish(topic string, header http.Header, body []byte) string {
	key := header.Get("Vqs-Idempotency-Key")
	if key != "" {
		for _, message := range q.messages {
			if message.idempotencyKey == key {
				return message.id
			}
		}
	}
	q.sequence++
	message := &localMessage{id: fmt.Sprintf("msg_%d", q.sequence), topic: topic, idempotencyKey: key, concurrencyKey: header.Get("Vqs-Experimental-Concurrency-Key"), body: body, visibleAt: time.Now()}
	if seconds, err := strconv.Atoi(header.Get("Vqs-Delay-Seconds")); err == nil {
		message.visibleAt = message.visibleAt.Add(time.Duration(seconds) * time.Second)
	}
	q.messages = append(q.messages, message)
	return message.id
}

// leased returns the message whose current lease is receipt.
func (q *localQueue) leased(receipt string) *localMessage {
	for _, message := range q.messages {
		if message.receipt == receipt && !message.done {
			return message
		}
	}
	return nil
}

// next leases the first message that is visible, coalescing a keyed one into
// its key's delivery, and returns the push request of its delivery, or nil
// when no message is visible.
func (q *localQueue) next() (*localMessage, *http.Request) {
	q.mu.Lock()
	defer q.mu.Unlock()
	now := time.Now()
	for _, message := range q.messages {
		if message.done || message.pushing || message.visibleAt.After(now) {
			continue
		}
		if holder := q.pushing(message.concurrencyKey); holder != nil {
			holder.coalesced++
			message.done = true
			continue
		}
		message.deliveries++
		message.receipt = fmt.Sprintf("rh_%s_%d", message.id, message.deliveries)
		message.visibleAt = now.Add(vercelLease)
		message.pushing = true
		request := httptest.NewRequest(http.MethodPost, "/queue", bytes.NewReader(message.body))
		request.Header.Set("Ce-Type", "com.vercel.queue.v2beta")
		request.Header.Set("Ce-Vqsqueuename", message.topic)
		request.Header.Set("Ce-Vqsconsumergroup", "api-queue")
		request.Header.Set("Ce-Vqsmessageid", message.id)
		request.Header.Set("Ce-Vqsreceipthandle", message.receipt)
		request.Header.Set("Ce-Vqsdeliverycount", strconv.Itoa(message.deliveries))
		if message.handoffs > 0 {
			request.Header.Set("Ce-Vqsexperimentalhandoffcount", strconv.Itoa(message.handoffs))
		}
		request.Header.Set("Content-Type", "application/json")
		return message, request
	}
	return nil, nil
}

// pushing returns the message of key whose delivery is being pushed.
func (q *localQueue) pushing(key string) *localMessage {
	if key == "" {
		return nil
	}
	for _, message := range q.messages {
		if message.pushing && message.concurrencyKey == key {
			return message
		}
	}
	return nil
}

// pushed ends the push of message's delivery.
func (q *localQueue) pushed(message *localMessage) {
	q.mu.Lock()
	defer q.mu.Unlock()
	message.pushing = false
}
