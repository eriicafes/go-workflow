// Package vercelworld provides the event-sourced Vercel World.
package vercelworld

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

type Config struct {
	RedisURL, RedisRESTURL, RedisRESTToken, QueueURL, OIDCToken, Topic, Prefix string
	HTTPClient                                                                 *http.Client
}
type World struct {
	*testworld.World
	config Config
	redis  redisStore
	key    string
}

func New(config Config, runners ...worlds.Runner) (*World, error) {
	if config.QueueURL == "" || config.OIDCToken == "" || config.Topic == "" {
		return nil, errors.New("workflow/vercel: queue, OIDC token, and topic are required")
	}
	store, err := newRedisStore(config.RedisURL, config.RedisRESTURL, config.RedisRESTToken, config.HTTPClient)
	if err != nil {
		return nil, err
	}
	prefix := config.Prefix
	if prefix == "" {
		prefix = "go-workflow"
	}
	w := &World{config: config, redis: store, key: prefix + ":world:state"}
	snapshot, err := w.load(context.Background())
	if err != nil {
		return nil, err
	}
	w.World = testworld.NewWithSnapshotAndQueue(snapshot, w.save, runners...)
	return w, nil
}

func (w *World) load(ctx context.Context) (testworld.Snapshot, error) {
	values, err := w.redis.HGetAll(ctx, w.key)
	if err != nil {
		return testworld.Snapshot{}, err
	}
	if values["state"] == "" {
		return testworld.Snapshot{}, nil
	}
	var snapshot testworld.Snapshot
	if err := json.Unmarshal([]byte(values["state"]), &snapshot); err != nil {
		return testworld.Snapshot{}, errors.New("workflow/vercel: invalid world state: " + err.Error())
	}
	return snapshot, nil
}

func (w *World) save(snapshot testworld.Snapshot) {
	data, err := json.Marshal(snapshot)
	if err == nil {
		_ = w.redis.HSet(context.Background(), w.key, "state", string(data))
	}
}

type queueMessage struct {
	ID    string            `json:"id"`
	Input worlds.QueueInput `json:"input"`
}

// Queue stores work in Redis before publishing it to the configured Vercel
// Queue endpoint. A delivery retry is safe because execution is event fenced.
func (w *World) Queue(ctx context.Context, input worlds.QueueInput) (worlds.QueueResult, error) {
	result, err := w.World.Queue(ctx, input)
	if err != nil {
		return worlds.QueueResult{}, err
	}
	body, err := json.Marshal(queueMessage{ID: result.MessageID, Input: input})
	if err != nil {
		return worlds.QueueResult{}, err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, w.config.QueueURL, strings.NewReader(string(body)))
	if err != nil {
		return worlds.QueueResult{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+w.config.OIDCToken)
	request.Header.Set("X-Vercel-Queue-Topic", w.config.Topic)
	client := w.config.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}
	response, err := client.Do(request)
	if err != nil {
		return worlds.QueueResult{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return worlds.QueueResult{}, errors.New("workflow/vercel: queue publish failed: " + response.Status)
	}
	return result, nil
}

func (w *World) QueueBatch(ctx context.Context, input worlds.QueueBatchInput) ([]worlds.QueueResult, error) {
	result := make([]worlds.QueueResult, 0, len(input.Items))
	for _, item := range input.Items {
		value, err := w.Queue(ctx, item)
		if err != nil {
			return result, err
		}
		result = append(result, value)
	}
	return result, nil
}

func HTTP(world *World) http.Handler { return worlds.HTTPHandler(world, nil, "") }
func Queue(world *World, runners worlds.RunnerSet) http.Handler {
	runtime := worlds.NewRuntime(world, runners)
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			writer.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		if request.Header.Get("Authorization") != "Bearer "+world.config.OIDCToken {
			http.Error(writer, "unauthorized queue delivery", http.StatusUnauthorized)
			return
		}
		defer request.Body.Close()
		body, err := io.ReadAll(http.MaxBytesReader(writer, request.Body, 1<<20))
		if err != nil {
			http.Error(writer, "invalid queue message", http.StatusBadRequest)
			return
		}
		var message queueMessage
		if err := json.Unmarshal(body, &message); err != nil || message.ID == "" {
			http.Error(writer, "invalid queue message", http.StatusBadRequest)
			return
		}
		if message.Input.NotBefore != nil && time.Until(*message.Input.NotBefore) > 0 {
			seconds := max(1, int(time.Until(*message.Input.NotBefore).Round(time.Second)/time.Second))
			writer.Header().Set("Retry-After", strconv.Itoa(seconds))
			http.Error(writer, "queue delivery is early", http.StatusTooEarly)
			return
		}
		if err := runtime.Execute(request.Context(), message.Input); err != nil {
			http.Error(writer, "workflow execution failed", http.StatusInternalServerError)
			return
		}
		if err := world.QueueCancel(request.Context(), worlds.QueueCancelInput{MessageID: message.ID}); err != nil && !errors.Is(err, worlds.ErrQueueMessageNotFound) {
			http.Error(writer, "queue acknowledgement failed", http.StatusInternalServerError)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	})
}
