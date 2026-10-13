package vercelworld

import (
	"bytes"
	"context"
	"encoding/base64"

	"github.com/eriicafes/go-workflow"
	"github.com/eriicafes/go-workflow/workflowops"
)

const appendStreamScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then return 'missing' end
if redis.call('EXISTS', KEYS[2]) == 0 then
	redis.call('HSET', KEYS[2], 'closed', '0', 'data', '')
end
if redis.call('HGET', KEYS[2], 'closed') == '1' then return 'closed' end
if redis.call('HGET', KEYS[2], 'lock') ~= ARGV[2] then return 'locked' end
redis.call('HSET', KEYS[2], 'data', (redis.call('HGET', KEYS[2], 'data') or '') .. ARGV[1])
return 'ok'
`

const lockStreamScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then return 'missing' end
if redis.call('EXISTS', KEYS[2]) == 0 then
  redis.call('HSET', KEYS[2], 'closed', '0', 'data', '')
end
if redis.call('HGET', KEYS[2], 'closed') == '1' then return 'closed' end
if redis.call('HGET', KEYS[2], 'lock') then return 'locked' end
redis.call('HSET', KEYS[2], 'lock', ARGV[1])
return 'ok'
`

const releaseStreamScript = `
if redis.call('EXISTS', KEYS[1]) == 0 then return 'missing' end
if redis.call('EXISTS', KEYS[2]) == 0 then
	return 'locked'
end
if redis.call('HGET', KEYS[2], 'lock') ~= ARGV[2] then return 'locked' end
if ARGV[1] == '1' then redis.call('HSET', KEYS[2], 'closed', '1') end
redis.call('HDEL', KEYS[2], 'lock')
return 'ok'
`

// ReadStream returns the current persisted byte range for reference.
func (world *World) ReadStream(ctx context.Context, reference workflowops.Stream, offset int64) (workflowops.StreamRead, error) {
	run, err := world.hash(ctx, world.runKey(reference.RunID))
	if err != nil {
		return workflowops.StreamRead{}, err
	}
	if len(run) == 0 {
		return workflowops.StreamRead{}, workflow.ErrRunNotFound
	}
	stream, err := world.hash(ctx, world.streamKey(reference))
	if err != nil {
		return workflowops.StreamRead{}, err
	}
	if len(stream) == 0 {
		return workflowops.StreamRead{Next: offset}, nil
	}
	data := []byte(stream["data"])
	if offset < 0 {
		offset = max(int64(len(data))+offset, 0)
	}
	if offset > int64(len(data)) {
		offset = int64(len(data))
	}
	return workflowops.StreamRead{
		Data:   bytes.Clone(data[offset:]),
		Next:   int64(len(data)),
		Closed: stream["closed"] == "1",
	}, nil
}

// LockStream acquires the stream's exclusive writer lock.
func (world *World) LockStream(ctx context.Context, reference workflowops.Stream, lock string) (bool, error) {
	result, err := world.eval(ctx, lockStreamScript, []string{world.runKey(reference.RunID), world.streamKey(reference)}, lock)
	if err != nil {
		return false, err
	}
	switch result {
	case "ok":
		return true, nil
	case "locked":
		return false, nil
	case "closed":
		return false, workflowops.ErrCancelled
	default:
		return false, workflow.ErrRunNotFound
	}
}

// AppendStream atomically appends bytes held by lock.
func (world *World) AppendStream(ctx context.Context, reference workflowops.Stream, lock string, data []byte) error {
	result, err := world.eval(ctx, appendStreamScript, []string{world.runKey(reference.RunID), world.streamKey(reference)}, string(data), lock)
	if err != nil {
		return err
	}
	if result == "ok" {
		return nil
	}
	if result == "closed" || result == "locked" {
		return workflowops.ErrCancelled
	}
	return workflow.ErrRunNotFound
}

// ReleaseStream releases token's writer lock and can close the stream.
func (world *World) ReleaseStream(ctx context.Context, reference workflowops.Stream, lock string, closeStream bool) error {
	closed := "0"
	if closeStream {
		closed = "1"
	}
	result, err := world.eval(ctx, releaseStreamScript, []string{world.runKey(reference.RunID), world.streamKey(reference)}, closed, lock)
	if err != nil {
		return err
	}
	if result == "ok" {
		return nil
	}
	if result == "locked" {
		return workflowops.ErrCancelled
	}
	return workflow.ErrRunNotFound
}

// CloseStreams closes every stream owned by a terminal run.
func (world *World) CloseStreams(ctx context.Context, runID string) error {
	prefix := world.config.Prefix + ":run:" + runID + ":stream:*"
	var cursor uint64
	for {
		keys, next, err := world.redis.Scan(ctx, cursor, prefix, 100)
		if err != nil {
			return err
		}
		for _, key := range keys {
			if err := world.redis.HSet(ctx, key, "closed", "1", "lock", ""); err != nil {
				return err
			}
		}
		if next == 0 {
			return nil
		}
		cursor = next
	}
}

func (world *World) streamKey(reference workflowops.Stream) string {
	namespace := base64.RawURLEncoding.EncodeToString([]byte(reference.Namespace))
	return world.config.Prefix + ":run:" + reference.RunID + ":stream:" + namespace
}
