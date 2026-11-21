package core

import (
	"sync"

	"github.com/eriicafes/go-workflow/worlds"
)

// A streak counts the in-band fence refusals of a run's flow deliveries in a
// row in this process, so a run whose every delivery is refused is reported
// instead of never. One refusal is expected; many in a row point at a World
// fault or a pathological write latency, which nothing else surfaces until
// the delivery cap fails the run.
type streak struct {
	mu    sync.Mutex
	runs  map[worlds.RunID]int
	order []worlds.RunID
}

const (
	// streakWarnThreshold is how many setbacks in a row a run has before one
	// is reported.
	streakWarnThreshold = 3
	// maxStreakRuns bounds the runs a streak remembers.
	maxStreakRuns = 1_000
)

var fenceRefusals = &streak{}

// note records a setback of runID and reports the run's streak, and whether
// to report it: at streakWarnThreshold, then at every power of two above it.
func (s *streak) note(runID worlds.RunID) (int, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.runs == nil {
		s.runs = map[worlds.RunID]int{}
	}
	if _, ok := s.runs[runID]; !ok {
		s.order = append(s.order, runID)
		for len(s.order) > maxStreakRuns {
			delete(s.runs, s.order[0])
			s.order = s.order[1:]
		}
	}
	s.runs[runID]++
	count := s.runs[runID]
	return count, count == streakWarnThreshold || count > streakWarnThreshold && count&(count-1) == 0
}

// clear ends runID's streak.
func (s *streak) clear(runID worlds.RunID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.runs, runID)
}
