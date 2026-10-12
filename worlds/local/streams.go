package localworld

import (
	"context"
	"encoding/hex"
	"errors"
	"io/fs"
	"path"

	"github.com/eriicafes/go-workflow/workflowops"
)

type streamRecord struct {
	Namespace string `json:"namespace"`
	Data      []byte `json:"data"`
	Closed    bool   `json:"closed"`
	Lock      string `json:"lock,omitempty"`
}

// ReadStream returns bytes at offset. Negative offsets remain unresolved until
// a stream exists, so readers opened before the first write remain relative to
// the eventual end.
func (world *World) ReadStream(_ context.Context, reference workflowops.Stream, offset int64) (workflowops.StreamRead, error) {
	if _, err := world.readRun(reference.RunID); err != nil {
		return workflowops.StreamRead{}, err
	}
	world.records.Lock()
	defer world.records.Unlock()
	stream, found, err := world.readStream(reference)
	if err != nil {
		return workflowops.StreamRead{}, err
	}
	if !found {
		return workflowops.StreamRead{Next: offset}, nil
	}
	if offset < 0 {
		offset = max(int64(len(stream.Data))+offset, 0)
	}
	if offset > int64(len(stream.Data)) {
		offset = int64(len(stream.Data))
	}
	return workflowops.StreamRead{
		Data:   clone(stream.Data[offset:]),
		Next:   int64(len(stream.Data)),
		Closed: stream.Closed,
	}, nil
}

// LockStream acquires the stream's exclusive writer lock.
func (world *World) LockStream(_ context.Context, reference workflowops.Stream, lock string) (bool, error) {
	if _, err := world.readRun(reference.RunID); err != nil {
		return false, err
	}
	world.records.Lock()
	defer world.records.Unlock()
	stream, found, err := world.readStream(reference)
	if err != nil {
		return false, err
	}
	if !found {
		stream = streamRecord{Namespace: reference.Namespace}
	}
	if stream.Closed {
		return false, workflowops.ErrCancelled
	}
	if stream.Lock != "" {
		return false, nil
	}
	stream.Lock = lock
	if err := writeJSON(world.root, streamName(reference), stream); err != nil {
		return false, err
	}
	world.signalLocked()
	return true, nil
}

// AppendStream durably appends bytes held by lock.
func (world *World) AppendStream(_ context.Context, reference workflowops.Stream, lock string, data []byte) error {
	world.records.Lock()
	defer world.records.Unlock()
	if _, err := world.readRun(reference.RunID); err != nil {
		return err
	}
	stream, found, err := world.readStream(reference)
	if err != nil {
		return err
	}
	if !found {
		stream = streamRecord{Namespace: reference.Namespace}
	}
	if stream.Closed || stream.Lock != lock {
		return workflowops.ErrCancelled
	}
	stream.Data = append(stream.Data, data...)
	if err := writeJSON(world.root, streamName(reference), stream); err != nil {
		return err
	}
	world.signalLocked()
	return nil
}

// ReleaseStream completes a writer lifetime and can close the stream.
func (world *World) ReleaseStream(_ context.Context, reference workflowops.Stream, lock string, closeStream bool) error {
	world.records.Lock()
	defer world.records.Unlock()
	if _, err := world.readRun(reference.RunID); err != nil {
		return err
	}
	stream, found, err := world.readStream(reference)
	if err != nil {
		return err
	}
	if !found || stream.Lock != lock {
		return workflowops.ErrCancelled
	}
	stream.Closed = stream.Closed || closeStream
	stream.Lock = ""
	if err := writeJSON(world.root, streamName(reference), stream); err != nil {
		return err
	}
	world.signalLocked()
	return nil
}

// CloseStreams closes every stream owned by runID.
func (world *World) CloseStreams(_ context.Context, runID string) error {
	world.records.Lock()
	defer world.records.Unlock()
	if err := world.closeStreamsLocked(runID); err != nil {
		return err
	}
	world.signalLocked()
	return nil
}

func (world *World) closeStreamsLocked(runID string) error {
	names, err := listJSON(world.root, streamDir(runID))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, name := range names {
		var stream streamRecord
		streamPath := path.Join(streamDir(runID), name)
		if err := readJSON(world.root, streamPath, &stream); err != nil {
			return err
		}
		stream.Closed = true
		stream.Lock = ""
		if err := writeJSON(world.root, streamPath, stream); err != nil {
			return err
		}
	}
	return nil
}

func (world *World) readStream(reference workflowops.Stream) (streamRecord, bool, error) {
	var stream streamRecord
	err := readJSON(world.root, streamName(reference), &stream)
	if errors.Is(err, fs.ErrNotExist) {
		return streamRecord{}, false, nil
	}
	if err != nil {
		return streamRecord{}, false, err
	}
	if stream.Namespace != reference.Namespace {
		return streamRecord{}, false, workflowops.HistoryMismatchError("stream", 0)
	}
	return stream, true, nil
}

func streamDir(runID string) string {
	return path.Join("streams", runID)
}

func streamName(reference workflowops.Stream) string {
	return path.Join(streamDir(reference.RunID), hex.EncodeToString([]byte(reference.Namespace))+".json")
}
