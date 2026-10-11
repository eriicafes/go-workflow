package vercelworld

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

type redisStore interface {
	HDel(context.Context, string, ...string) error
	HSet(context.Context, string, ...any) error
	HGetAll(context.Context, string) (map[string]string, error)
	SMembers(context.Context, string) ([]string, error)
	Incr(context.Context, string) (int64, error)
	Scan(context.Context, uint64, string, int64) ([]string, uint64, error)
	ZRangeByScore(context.Context, string, string, string) ([]string, error)
	Eval(context.Context, string, []string, ...any) (any, error)
}

func newRedisStore(url, restURL, restToken string, client *http.Client) (redisStore, error) {
	if url != "" {
		options, err := redis.ParseURL(url)
		if err != nil {
			return nil, fmt.Errorf("workflow/vercel: parse Redis TCP URL: %w", err)
		}
		if options.TLSConfig == nil {
			options.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		return tcpRedis{client: redis.NewClient(options)}, nil
	}
	if restURL == "" || restToken == "" {
		return nil, errors.New("workflow/vercel: Redis URL or Redis REST URL and token are required")
	}
	if client == nil {
		client = http.DefaultClient
	}
	return restRedis{
		url:   strings.TrimRight(restURL, "/"),
		token: restToken,
		http:  client,
	}, nil
}

type tcpRedis struct {
	client *redis.Client
}

func (store tcpRedis) HDel(ctx context.Context, key string, fields ...string) error {
	return store.client.HDel(ctx, key, fields...).Err()
}

func (store tcpRedis) HSet(ctx context.Context, key string, values ...any) error {
	return store.client.HSet(ctx, key, values...).Err()
}

func (store tcpRedis) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	return store.client.HGetAll(ctx, key).Result()
}

func (store tcpRedis) SMembers(ctx context.Context, key string) ([]string, error) {
	return store.client.SMembers(ctx, key).Result()
}

func (store tcpRedis) Incr(ctx context.Context, key string) (int64, error) {
	return store.client.Incr(ctx, key).Result()
}

func (store tcpRedis) Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error) {
	return store.client.Scan(ctx, cursor, match, count).Result()
}

func (store tcpRedis) ZRangeByScore(ctx context.Context, key, min, max string) ([]string, error) {
	return store.client.ZRangeByScore(ctx, key, &redis.ZRangeBy{Min: min, Max: max}).Result()
}

func (store tcpRedis) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	return store.client.Eval(ctx, script, keys, args...).Result()
}

type restRedis struct {
	url   string
	token string
	http  *http.Client
}

func (store restRedis) HDel(ctx context.Context, key string, fields ...string) error {
	arguments := make([]any, 0, 1+len(fields))
	arguments = append(arguments, key)
	for _, field := range fields {
		arguments = append(arguments, field)
	}
	_, err := store.command(ctx, "HDEL", arguments...)
	return err
}

func (store restRedis) HSet(ctx context.Context, key string, values ...any) error {
	_, err := store.command(ctx, "HSET", append([]any{key}, values...)...)
	return err
}

func (store restRedis) HGetAll(ctx context.Context, key string) (map[string]string, error) {
	result, err := store.command(ctx, "HGETALL", key)
	if err != nil {
		return nil, err
	}
	var fields map[string]string
	if err := json.Unmarshal(result, &fields); err == nil {
		return fields, nil
	}
	var values []string
	if err := json.Unmarshal(result, &values); err != nil {
		return nil, fmt.Errorf("workflow/vercel: decode Redis hash: %w", err)
	}
	if len(values)%2 != 0 {
		return nil, errors.New("workflow/vercel: Redis hash has an odd field count")
	}
	fields = make(map[string]string, len(values)/2)
	for index := 0; index < len(values); index += 2 {
		fields[values[index]] = values[index+1]
	}
	return fields, nil
}

func (store restRedis) SMembers(ctx context.Context, key string) ([]string, error) {
	result, err := store.command(ctx, "SMEMBERS", key)
	if err != nil {
		return nil, err
	}
	var members []string
	if err := json.Unmarshal(result, &members); err != nil {
		return nil, fmt.Errorf("workflow/vercel: decode Redis set: %w", err)
	}
	return members, nil
}

func (store restRedis) Incr(ctx context.Context, key string) (int64, error) {
	result, err := store.command(ctx, "INCR", key)
	if err != nil {
		return 0, err
	}
	var value int64
	if err := json.Unmarshal(result, &value); err != nil {
		return 0, fmt.Errorf("workflow/vercel: decode Redis increment: %w", err)
	}
	return value, nil
}

func (store restRedis) Scan(ctx context.Context, cursor uint64, match string, count int64) ([]string, uint64, error) {
	result, err := store.command(ctx, "SCAN", strconv.FormatUint(cursor, 10), "MATCH", match, "COUNT", count)
	if err != nil {
		return nil, 0, err
	}
	var values []json.RawMessage
	if err := json.Unmarshal(result, &values); err != nil || len(values) != 2 {
		return nil, 0, errors.New("workflow/vercel: decode Redis scan result")
	}
	var next string
	if err := json.Unmarshal(values[0], &next); err != nil {
		return nil, 0, fmt.Errorf("workflow/vercel: decode Redis scan cursor: %w", err)
	}
	nextCursor, err := strconv.ParseUint(next, 10, 64)
	if err != nil {
		return nil, 0, fmt.Errorf("workflow/vercel: parse Redis scan cursor: %w", err)
	}
	var keys []string
	if err := json.Unmarshal(values[1], &keys); err != nil {
		return nil, 0, fmt.Errorf("workflow/vercel: decode Redis scan keys: %w", err)
	}
	return keys, nextCursor, nil
}

func (store restRedis) ZRangeByScore(ctx context.Context, key, min, max string) ([]string, error) {
	result, err := store.command(ctx, "ZRANGEBYSCORE", key, min, max)
	if err != nil {
		return nil, err
	}
	var values []string
	if err := json.Unmarshal(result, &values); err != nil {
		return nil, fmt.Errorf("workflow/vercel: decode Redis sorted set: %w", err)
	}
	return values, nil
}

func (store restRedis) Eval(ctx context.Context, script string, keys []string, args ...any) (any, error) {
	values := make([]any, 0, 2+len(keys)+len(args))
	values = append(values, script, len(keys))
	for _, key := range keys {
		values = append(values, key)
	}
	values = append(values, args...)
	result, err := store.command(ctx, "EVAL", values...)
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader(result))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("workflow/vercel: decode Redis Lua result: %w", err)
	}
	if number, ok := value.(json.Number); ok {
		integer, err := number.Int64()
		if err != nil {
			return nil, fmt.Errorf("workflow/vercel: decode Redis Lua integer: %w", err)
		}
		return integer, nil
	}
	return value, nil
}

func (store restRedis) command(ctx context.Context, name string, arguments ...any) (json.RawMessage, error) {
	command := append([]any{name}, arguments...)
	body, err := json.Marshal(command)
	if err != nil {
		return nil, fmt.Errorf("workflow/vercel: encode Redis command: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, store.url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	request.Header.Set("Authorization", "Bearer "+store.token)
	request.Header.Set("Content-Type", "application/json")
	response, err := store.http.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil {
		return nil, err
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("workflow/vercel: Redis REST command %s: %s: %s", name, response.Status, strings.TrimSpace(string(data)))
	}
	var result struct {
		Result json.RawMessage `json:"result"`
		Error  string          `json:"error"`
	}
	if err := json.Unmarshal(data, &result); err != nil {
		return nil, fmt.Errorf("workflow/vercel: decode Redis REST response: %w", err)
	}
	if result.Error != "" {
		return nil, fmt.Errorf("workflow/vercel: Redis command %s: %s", name, result.Error)
	}
	return result.Result, nil
}
