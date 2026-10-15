package vercelworld

import (
	"bytes"
	"context"
	"crypto/sha1"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// restResponseMaxBytes caps one Redis REST response. Larger responses return
// an error instead of being truncated.
var restResponseMaxBytes = 64 << 20

// redisScript is a Lua script addressed by its SHA1 digest.
type redisScript struct {
	src string
	sha string
}

func newRedisScript(src string) *redisScript {
	sum := sha1.Sum([]byte(src))
	return &redisScript{src: src, sha: hex.EncodeToString(sum[:])}
}

type redisZ struct {
	Member string
	Score  float64
}

// redisStore is the command surface the World uses over TCP and REST. Every
// mutation is one Lua script. Scripts receive no KEYS and derive keys from the
// prefix in ARGV[1], so the database must not be a Redis Cluster.
type redisStore interface {
	// Run executes script with EVALSHA and falls back to EVAL on NOSCRIPT.
	// Results are nil, int64, string, or []any of those.
	Run(ctx context.Context, script *redisScript, args ...string) (any, error)
	Get(ctx context.Context, key string) (string, bool, error)
	// MGet and HMGet return "" for missing values.
	MGet(ctx context.Context, keys ...string) ([]string, error)
	HGet(ctx context.Context, key, field string) (string, bool, error)
	HMGet(ctx context.Context, key string, fields ...string) ([]string, error)
	LIndex(ctx context.Context, key string, index int64) (string, bool, error)
	LRange(ctx context.Context, key string, start, stop int64) ([]string, error)
	// ZRangeByScore returns members with scores in [min, max] (Redis score
	// syntax), descending when rev is set. A negative count returns all.
	ZRangeByScore(ctx context.Context, key, min, max string, rev bool, count int64) ([]redisZ, error)
	SMembers(ctx context.Context, key string) ([]string, error)
	Exists(ctx context.Context, key string) (bool, error)
	Close() error
}

func newRedisStore(url, restURL, restToken string, client *http.Client) (redisStore, error) {
	if url != "" {
		options, err := redis.ParseURL(url)
		if err != nil {
			return nil, fmt.Errorf("workflow/vercel: parse Redis TCP URL: %w", err)
		}
		// redis.ParseURL enables TLS only for rediss:// URLs.
		if options.TLSConfig != nil && options.TLSConfig.MinVersion == 0 {
			options.TLSConfig.MinVersion = tls.VersionTLS12
		}
		return tcpRedis{client: redis.NewClient(options)}, nil
	}
	if restURL == "" || restToken == "" {
		return nil, errors.New("workflow/vercel: Redis URL or Redis REST URL and token are required")
	}
	if client == nil {
		client = http.DefaultClient
	}
	return restRedis{url: strings.TrimRight(restURL, "/"), token: restToken, http: client}, nil
}

type tcpRedis struct {
	client *redis.Client
}

func (store tcpRedis) Close() error { return store.client.Close() }

func (store tcpRedis) Run(ctx context.Context, script *redisScript, args ...string) (any, error) {
	values := make([]any, len(args))
	for index, arg := range args {
		values[index] = arg
	}
	result, err := store.client.EvalSha(ctx, script.sha, nil, values...).Result()
	if err != nil && redis.HasErrorPrefix(err, "NOSCRIPT") {
		result, err = store.client.Eval(ctx, script.src, nil, values...).Result()
	}
	if errors.Is(err, redis.Nil) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("workflow/vercel: Redis script: %w", err)
	}
	return normalizeReply(result), nil
}

func (store tcpRedis) Get(ctx context.Context, key string) (string, bool, error) {
	return optional(store.client.Get(ctx, key).Result())
}

func (store tcpRedis) MGet(ctx context.Context, keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	values, err := store.client.MGet(ctx, keys...).Result()
	return sliceStrings(values), err
}

func (store tcpRedis) HGet(ctx context.Context, key, field string) (string, bool, error) {
	return optional(store.client.HGet(ctx, key, field).Result())
}

func (store tcpRedis) HMGet(ctx context.Context, key string, fields ...string) ([]string, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	values, err := store.client.HMGet(ctx, key, fields...).Result()
	return sliceStrings(values), err
}

func (store tcpRedis) LIndex(ctx context.Context, key string, index int64) (string, bool, error) {
	return optional(store.client.LIndex(ctx, key, index).Result())
}

func (store tcpRedis) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return store.client.LRange(ctx, key, start, stop).Result()
}

func (store tcpRedis) ZRangeByScore(ctx context.Context, key, min, max string, rev bool, count int64) ([]redisZ, error) {
	by := &redis.ZRangeBy{Min: min, Max: max, Count: count}
	var values []redis.Z
	var err error
	if rev {
		values, err = store.client.ZRevRangeByScoreWithScores(ctx, key, by).Result()
	} else {
		values, err = store.client.ZRangeByScoreWithScores(ctx, key, by).Result()
	}
	if err != nil {
		return nil, err
	}
	result := make([]redisZ, len(values))
	for index, value := range values {
		member, _ := value.Member.(string)
		result[index] = redisZ{Member: member, Score: value.Score}
	}
	return result, nil
}

func (store tcpRedis) SMembers(ctx context.Context, key string) ([]string, error) {
	return store.client.SMembers(ctx, key).Result()
}

func (store tcpRedis) Exists(ctx context.Context, key string) (bool, error) {
	count, err := store.client.Exists(ctx, key).Result()
	return count > 0, err
}

// restRedis speaks the Upstash Redis REST protocol, where each command is a
// JSON array POSTed to the endpoint and answered with {"result"} or {"error"}.
type restRedis struct {
	url   string
	token string
	http  *http.Client
}

func (restRedis) Close() error { return nil }

func (store restRedis) Run(ctx context.Context, script *redisScript, args ...string) (any, error) {
	command := make([]any, 0, len(args)+3)
	command = append(command, "EVALSHA", script.sha, "0")
	for _, arg := range args {
		command = append(command, arg)
	}
	result, err := store.do(ctx, command)
	var replyErr restError
	if errors.As(err, &replyErr) && strings.HasPrefix(string(replyErr), "NOSCRIPT") {
		command[0], command[1] = "EVAL", script.src
		result, err = store.do(ctx, command)
	}
	if err != nil {
		return nil, fmt.Errorf("workflow/vercel: Redis script: %w", err)
	}
	return decodeREST(result)
}

func (store restRedis) Get(ctx context.Context, key string) (string, bool, error) {
	return store.optional(ctx, "GET", key)
}

func (store restRedis) MGet(ctx context.Context, keys ...string) ([]string, error) {
	if len(keys) == 0 {
		return nil, nil
	}
	return store.strings(ctx, append([]any{"MGET"}, anys(keys)...)...)
}

func (store restRedis) HGet(ctx context.Context, key, field string) (string, bool, error) {
	return store.optional(ctx, "HGET", key, field)
}

func (store restRedis) HMGet(ctx context.Context, key string, fields ...string) ([]string, error) {
	if len(fields) == 0 {
		return nil, nil
	}
	return store.strings(ctx, append([]any{"HMGET", key}, anys(fields)...)...)
}

func (store restRedis) LIndex(ctx context.Context, key string, index int64) (string, bool, error) {
	return store.optional(ctx, "LINDEX", key, strconv.FormatInt(index, 10))
}

func (store restRedis) LRange(ctx context.Context, key string, start, stop int64) ([]string, error) {
	return store.strings(ctx, "LRANGE", key, strconv.FormatInt(start, 10), strconv.FormatInt(stop, 10))
}

func (store restRedis) ZRangeByScore(ctx context.Context, key, min, max string, rev bool, count int64) ([]redisZ, error) {
	command := []any{"ZRANGEBYSCORE", key, min, max, "WITHSCORES"}
	if rev {
		command = []any{"ZREVRANGEBYSCORE", key, max, min, "WITHSCORES"}
	}
	if count >= 0 {
		command = append(command, "LIMIT", "0", strconv.FormatInt(count, 10))
	}
	values, err := store.strings(ctx, command...)
	if err != nil {
		return nil, err
	}
	if len(values)%2 != 0 {
		return nil, errors.New("workflow/vercel: Redis sorted set reply has an odd length")
	}
	result := make([]redisZ, 0, len(values)/2)
	for index := 0; index < len(values); index += 2 {
		score, err := strconv.ParseFloat(values[index+1], 64)
		if err != nil {
			return nil, fmt.Errorf("workflow/vercel: decode Redis score: %w", err)
		}
		result = append(result, redisZ{Member: values[index], Score: score})
	}
	return result, nil
}

func (store restRedis) SMembers(ctx context.Context, key string) ([]string, error) {
	return store.strings(ctx, "SMEMBERS", key)
}

func (store restRedis) Exists(ctx context.Context, key string) (bool, error) {
	result, err := store.do(ctx, []any{"EXISTS", key})
	if err != nil {
		return false, err
	}
	value, err := decodeREST(result)
	if err != nil {
		return false, err
	}
	count, ok := value.(int64)
	if !ok {
		return false, fmt.Errorf("workflow/vercel: unexpected Redis EXISTS reply %T", value)
	}
	return count > 0, nil
}

func (store restRedis) optional(ctx context.Context, command ...any) (string, bool, error) {
	result, err := store.do(ctx, command)
	if err != nil {
		return "", false, err
	}
	value, err := decodeREST(result)
	if err != nil || value == nil {
		return "", false, err
	}
	text, ok := value.(string)
	if !ok {
		return "", false, fmt.Errorf("workflow/vercel: unexpected Redis %s reply %T", command[0], value)
	}
	return text, true, nil
}

func (store restRedis) strings(ctx context.Context, command ...any) ([]string, error) {
	result, err := store.do(ctx, command)
	if err != nil {
		return nil, err
	}
	value, err := decodeREST(result)
	if err != nil || value == nil {
		return nil, err
	}
	values, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("workflow/vercel: unexpected Redis %s reply %T", command[0], value)
	}
	strs := make([]string, len(values))
	for index, item := range values {
		switch item := item.(type) {
		case nil:
		case string:
			strs[index] = item
		case int64:
			strs[index] = strconv.FormatInt(item, 10)
		default:
			return nil, fmt.Errorf("workflow/vercel: unexpected Redis %s item %T", command[0], item)
		}
	}
	return strs, nil
}

// restError is an error reply returned by Redis itself.
type restError string

func (err restError) Error() string { return string(err) }

func (store restRedis) do(ctx context.Context, command []any) (json.RawMessage, error) {
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
		return nil, fmt.Errorf("workflow/vercel: Redis REST %v: %w", command[0], err)
	}
	defer response.Body.Close()
	data, err := io.ReadAll(io.LimitReader(response.Body, int64(restResponseMaxBytes)+1))
	if err != nil {
		return nil, fmt.Errorf("workflow/vercel: read Redis REST response: %w", err)
	}
	if len(data) > restResponseMaxBytes {
		return nil, fmt.Errorf("workflow/vercel: Redis REST %v response exceeds %d bytes", command[0], restResponseMaxBytes)
	}
	var reply struct {
		Result json.RawMessage `json:"result"`
		Error  *string         `json:"error"`
	}
	if err := json.Unmarshal(data, &reply); err != nil {
		if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
			return nil, fmt.Errorf("workflow/vercel: Redis REST %v: %s: %s", command[0], response.Status, strings.TrimSpace(string(data)))
		}
		return nil, fmt.Errorf("workflow/vercel: decode Redis REST response: %w", err)
	}
	if reply.Error != nil {
		return nil, restError(*reply.Error)
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("workflow/vercel: Redis REST %v: %s", command[0], response.Status)
	}
	return reply.Result, nil
}

func normalizeReply(value any) any {
	switch value := value.(type) {
	case []any:
		for index := range value {
			value[index] = normalizeReply(value[index])
		}
		return value
	case int:
		return int64(value)
	default:
		return value
	}
}

func optional(value string, err error) (string, bool, error) {
	if errors.Is(err, redis.Nil) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return value, true, nil
}

func sliceStrings(values []any) []string {
	result := make([]string, len(values))
	for index, value := range values {
		result[index], _ = value.(string)
	}
	return result
}

// decodeREST converts a JSON reply to the shapes tcpRedis returns.
func decodeREST(raw json.RawMessage) (any, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, fmt.Errorf("workflow/vercel: decode Redis REST result: %w", err)
	}
	return restValue(value)
}

func restValue(value any) (any, error) {
	switch value := value.(type) {
	case nil, string:
		return value, nil
	case json.Number:
		integer, err := value.Int64()
		if err != nil {
			return nil, fmt.Errorf("workflow/vercel: decode Redis integer %q: %w", value, err)
		}
		return integer, nil
	case []any:
		for index, item := range value {
			converted, err := restValue(item)
			if err != nil {
				return nil, err
			}
			value[index] = converted
		}
		return value, nil
	default:
		return nil, fmt.Errorf("workflow/vercel: unexpected Redis REST value %T", value)
	}
}

func anys(values []string) []any {
	result := make([]any, len(values))
	for index, value := range values {
		result[index] = value
	}
	return result
}
