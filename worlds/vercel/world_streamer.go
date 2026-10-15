package vercelworld

import (
	"context"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/eriicafes/go-workflow/worlds"
)

const streamPollMax = 500 * time.Millisecond

type streamWriteSession struct {
	mu     sync.Mutex
	world  *World
	runID  worlds.RunID
	name   string
	id     worlds.StreamID
	closed bool
}

func (s *streamWriteSession) ID() worlds.StreamID { return s.id }

func (s *streamWriteSession) Write(ctx context.Context, input worlds.StreamWriteSessionWriteInput) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return worlds.ErrRunTerminal
	}
	return s.world.StreamsWriteSession(ctx, worlds.StreamsWriteSessionInput{RunID: s.runID, Name: s.name, SessionID: s.id, Sequence: input.Sequence, Chunks: input.Chunks})
}

func (s *streamWriteSession) Close(ctx context.Context) error   { return s.close(ctx, true) }
func (s *streamWriteSession) Release(ctx context.Context) error { return s.close(ctx, false) }

func (s *streamWriteSession) close(ctx context.Context, closeStream bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	if err := s.world.StreamsCloseWriteSession(ctx, worlds.StreamsCloseWriteSessionInput{RunID: s.runID, Name: s.name, SessionID: s.id, Close: closeStream}); err != nil {
		return err
	}
	s.closed = true
	return nil
}

func (w *World) streamScript(ctx context.Context, script *redisScript, runID worlds.RunID, name string, args ...string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	_, err := scriptReply(w.redis.Run(ctx, script, append([]string{w.prefix, string(runID), name}, args...)...))
	return err
}

func (w *World) StreamsCreateWriteSession(ctx context.Context, input worlds.StreamsCreateWriteSessionInput) (worlds.StreamWriteSession, error) {
	id := worlds.NewStreamID()
	if err := w.streamScript(ctx, scriptStreamCreateSession, input.RunID, input.Name, string(id)); err != nil {
		return nil, err
	}
	return &streamWriteSession{world: w, runID: input.RunID, name: input.Name, id: id}, nil
}

func (w *World) StreamsWriteSession(ctx context.Context, input worlds.StreamsWriteSessionInput) error {
	args := append([]string{string(input.SessionID), strconv.Itoa(input.Sequence)}, encodeChunks(input.Chunks)...)
	return w.streamScript(ctx, scriptStreamWriteSession, input.RunID, input.Name, args...)
}

func (w *World) StreamsCloseWriteSession(ctx context.Context, input worlds.StreamsCloseWriteSessionInput) error {
	closeStream := "0"
	if input.Close {
		closeStream = "1"
	}
	return w.streamScript(ctx, scriptStreamCloseSession, input.RunID, input.Name, string(input.SessionID), closeStream)
}

func (w *World) StreamsWrite(ctx context.Context, input worlds.StreamsWriteInput) error {
	return w.StreamsWriteMulti(ctx, worlds.StreamsWriteMultiInput{RunID: input.RunID, Name: input.Name, Chunks: [][]byte{input.Chunk}})
}

func (w *World) StreamsWriteMulti(ctx context.Context, input worlds.StreamsWriteMultiInput) error {
	return w.streamScript(ctx, scriptStreamWrite, input.RunID, input.Name, encodeChunks(input.Chunks)...)
}

func (w *World) StreamsClose(ctx context.Context, input worlds.StreamsCloseInput) error {
	return w.streamScript(ctx, scriptStreamClose, input.RunID, input.Name)
}

// readStream returns the done flag, chunk count, and up to count chunks from
// start in one consistent read.
func (w *World) readStream(ctx context.Context, runID worlds.RunID, name string, start, count int) (bool, int, [][]byte, error) {
	result, err := w.redis.Run(ctx, scriptStreamRead, w.prefix, string(runID), name, strconv.Itoa(start), strconv.Itoa(count))
	if err != nil {
		return false, 0, nil, err
	}
	values, _ := result.([]any)
	if len(values) < 2 {
		return false, 0, nil, errCorrupt
	}
	chunks := make([][]byte, 0, len(values)-2)
	for _, value := range values[2:] {
		chunk, err := decodeBytes(replyString(value))
		if err != nil {
			return false, 0, nil, err
		}
		if chunk == nil {
			chunk = []byte{}
		}
		chunks = append(chunks, chunk)
	}
	return replyString(values[0]) == "1", int(replyInt(values[1])), chunks, nil
}

func (w *World) StreamsGetChunks(ctx context.Context, input worlds.StreamsGetChunksInput) (worlds.StreamChunksResult, error) {
	// Poll with a backoff from 50ms to 500ms while waiting.
	start := 0
	if input.Cursor != "" {
		value, err := strconv.Atoi(input.Cursor)
		if err != nil || value < 0 {
			return worlds.StreamChunksResult{}, worlds.ErrEventInvalid
		}
		start = value
	}
	limit := input.Limit
	if limit <= 0 {
		limit = 100
	}
	deadline := time.Now().Add(input.Wait)
	delay := 50 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return worlds.StreamChunksResult{}, err
		}
		done, length, chunks, err := w.readStream(ctx, input.RunID, input.Name, start, limit)
		if err != nil {
			return worlds.StreamChunksResult{}, err
		}
		remaining := time.Until(deadline)
		if len(chunks) > 0 || done || input.Wait <= 0 || remaining <= 0 {
			end := start + len(chunks)
			result := worlds.StreamChunksResult{Done: done, HasMore: end < length, RetryAfter: delay, Cursor: strconv.Itoa(max(end, start))}
			for index, chunk := range chunks {
				result.Data = append(result.Data, worlds.StreamChunk{Index: start + index, Data: chunk})
			}
			return result, nil
		}
		if err := sleep(ctx, min(delay, remaining)); err != nil {
			return worlds.StreamChunksResult{}, err
		}
		delay = min(delay*2, streamPollMax)
	}
}

func (w *World) StreamsGetInfo(ctx context.Context, input worlds.StreamsGetInfoInput) (worlds.StreamInfo, error) {
	if err := ctx.Err(); err != nil {
		return worlds.StreamInfo{}, err
	}
	done, length, _, err := w.readStream(ctx, input.RunID, input.Name, 0, 0)
	if err != nil {
		return worlds.StreamInfo{}, err
	}
	return worlds.StreamInfo{TailIndex: length - 1, Done: done}, nil
}

func (w *World) StreamsList(ctx context.Context, input worlds.StreamsListInput) ([]string, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	names, err := w.redis.SMembers(ctx, w.key("streams", string(input.RunID)))
	if err != nil {
		return nil, err
	}
	sort.Strings(names)
	if names == nil {
		names = []string{}
	}
	return names, nil
}

func encodeChunks(chunks [][]byte) []string {
	values := make([]string, len(chunks))
	for index, chunk := range chunks {
		values[index] = encodeBytes(chunk)
	}
	return values
}
