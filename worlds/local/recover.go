package localworld

import (
	"encoding/json"
	"path"
	"strings"

	"github.com/eriicafes/go-workflow/worlds"
)

// recoverLocked restores the commit invariants after an interrupted process.
// Event files past a run's EventCount are discarded with the views they
// touched, partially created runs are removed, and stream sessions, which only
// live as long as the process that opened them, are released.
func (w *World) recoverLocked() error {
	runs, err := w.readRuns()
	if err != nil {
		return err
	}
	committed := make(map[string]bool, len(runs))
	for _, run := range runs {
		committed[fileKey(string(run.RunID))] = true
		uncommitted, err := exists(w.root, eventPath(run.RunID, run.EventCount+1))
		if err != nil {
			return err
		}
		if uncommitted {
			if err := w.rebuildRun(run.RunID); err != nil {
				return err
			}
		}
	}
	for _, parent := range []string{"events", "steps"} {
		dirs, err := listDirs(w.root, parent)
		if err != nil {
			return err
		}
		for _, dir := range dirs {
			if !committed[dir] {
				if err := w.root.RemoveAll(path.Join(parent, dir)); err != nil {
					return err
				}
			}
		}
	}
	return w.releaseStreamSessions()
}

// rebuildRun discards event files past the committed EventCount and rewrites
// the run's steps, hooks, and token claims by replaying the committed events.
// Discarded files are removed last, highest slot first, so an interrupted
// rebuild is detected and repeated by the next Prepare.
func (w *World) rebuildRun(runID worlds.RunID) error {
	run, err := w.readRun(runID)
	if err != nil {
		return err
	}
	dir := eventsDir(runID)
	names, err := listJSON(w.root, dir)
	if err != nil {
		return err
	}
	// Hooks and tokens named by discarded hook_created events may have been
	// written before the crash.
	var discarded []string
	discardedHooks := map[worlds.HookID]bool{}
	tokens := map[string]bool{}
	for _, name := range names {
		slot, err := worlds.EventSlot(worlds.EventID(strings.TrimSuffix(name, ".json")))
		if err != nil || int(slot) <= run.EventCount {
			continue
		}
		discarded = append(discarded, name)
		var event worlds.Event
		if readJSON(w.root, path.Join(dir, name), &event) != nil || event.EventType != worlds.EventTypeHookCreated {
			continue
		}
		discardedHooks[worlds.HookID(event.CorrelationID)] = true
		var created hookCreatedData
		if json.Unmarshal(event.Data, &created) == nil && created.Token != "" {
			tokens[created.Token] = true
		}
	}

	events, err := w.readEvents(runID, 1, run.EventCount)
	if err != nil {
		return err
	}
	replay := newTxn(w, run, true)
	for _, event := range events {
		if err := replay.materialize(event); err != nil {
			return err
		}
	}

	if err := w.root.RemoveAll(stepsDir(runID)); err != nil {
		return err
	}
	for _, step := range replay.steps {
		if err := writeJSON(w.root, stepPath(runID, step.StepID), step); err != nil {
			return err
		}
	}
	ownHooks := map[worlds.HookID]bool{}
	for id, hook := range replay.hooks {
		ownHooks[id] = true
		tokens[hook.Token] = true
		if err := writeJSON(w.root, hookPath(id), hook); err != nil {
			return err
		}
	}
	for id := range discardedHooks {
		if ownHooks[id] {
			continue
		}
		if hook, err := w.readHook(id); err != nil {
			return err
		} else if hook != nil && hook.RunID == runID {
			if err := remove(w.root, hookPath(id)); err != nil {
				return err
			}
		}
		ownHooks[id] = true
	}
	// Only rewrite claims this run could have made. A token that now belongs
	// to another run's hook was legitimately claimed after this run released it.
	for token := range tokens {
		if token == "" {
			continue
		}
		current, err := w.readToken(token)
		if err != nil {
			return err
		}
		if current != nil && !ownHooks[current.HookID] {
			if hook, err := w.readHook(current.HookID); err != nil {
				return err
			} else if hook != nil {
				continue
			}
		}
		if value := replay.tokens[token]; value != nil {
			err = writeJSON(w.root, tokenPath(token), value)
		} else {
			err = remove(w.root, tokenPath(token))
		}
		if err != nil {
			return err
		}
	}

	for index := len(discarded) - 1; index >= 0; index-- {
		if err := remove(w.root, path.Join(dir, discarded[index])); err != nil {
			return err
		}
	}
	return nil
}

func (w *World) releaseStreamSessions() error {
	runs, err := listDirs(w.root, "streams")
	if err != nil {
		return err
	}
	for _, run := range runs {
		names, err := listDirs(w.root, path.Join("streams", run))
		if err != nil {
			return err
		}
		for _, name := range names {
			metaPath := path.Join("streams", run, name, "meta.json")
			var meta streamMeta
			ok, err := w.readOptional(metaPath, &meta)
			if err != nil {
				return err
			}
			if ok && meta.Session != "" {
				meta.Session, meta.Sequence = "", 0
				if err := writeJSON(w.root, metaPath, meta); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
