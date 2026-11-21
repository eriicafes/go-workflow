package worlds

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf16"

	"github.com/oklog/ulid/v2"
)

// runSeed draws every ID, token, and random value a run's workflow code uses
// from one generator seeded with the run, so each replay draws the same
// values in the same order. The generator is seedrandom's ARC4, seeded with
// "<run ID>:<workflow name>:<deployment ID>". Correlation IDs are monotonic
// ULIDs at the run's creation time: the first draws 16 values and each later
// one is the previous ULID plus one, drawing nothing. Hook tokens are 21
// characters of Nano ID's URL alphabet read from the end of 34 drawn bytes.
type runSeed struct {
	random arc4Random
	time   uint64
	last   [ulidRandomLength]byte
	minted bool
}

// newRunSeed returns the generator of run, which needs a run ID of wrun_ and
// a ULID for the time of its correlation IDs.
func newRunSeed(run Run) (*runSeed, error) {
	created, err := runCreatedAt(run.RunID)
	if err != nil {
		return nil, err
	}
	seed := &runSeed{time: created}
	seed.random.init(string(run.RunID) + ":" + run.WorkflowName + ":" + run.DeploymentID)
	return seed, nil
}

// runCreatedAt returns the creation time encoded in a run ID, in Unix
// milliseconds: the time of its ULID with the region tag bit cleared.
func runCreatedAt(id RunID) (uint64, error) {
	rest, ok := strings.CutPrefix(string(id), "wrun_")
	if !ok {
		return 0, fmt.Errorf("workflow: run ID %q is not wrun_ and a ULID", id)
	}
	value, err := ulid.ParseStrict(rest)
	if err != nil {
		return 0, fmt.Errorf("workflow: run ID %q is not wrun_ and a ULID: %w", id, err)
	}
	value[0] &^= 0x80
	return value.Time(), nil
}

// float64 returns the next value in [0, 1).
func (s *runSeed) float64() float64 { return s.random.next() }

// Read fills p with drawn bytes, each the floor of a value times 256.
func (s *runSeed) Read(p []byte) (int, error) {
	for index := range p {
		p[index] = byte(s.random.next() * 256)
	}
	return len(p), nil
}

const (
	ulidEncoding     = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	ulidTimeLength   = 10
	ulidRandomLength = 16
)

// errULIDOverflow reports that the run's monotonic ULIDs ran out.
var errULIDOverflow = errors.New("workflow: correlation ID sequence overflowed")

// ulid returns the next correlation ULID.
func (s *runSeed) ulid() string {
	if !s.minted {
		// Each drawn character is prepended, so the first draw ends last.
		for index := ulidRandomLength - 1; index >= 0; index-- {
			s.last[index] = ulidEncoding[int(s.random.next()*32)%32]
		}
		s.minted = true
	} else if !incrementBase32(s.last[:]) {
		panic(errULIDOverflow)
	}
	var id [ulidTimeLength + ulidRandomLength]byte
	time := s.time
	for index := ulidTimeLength - 1; index >= 0; index-- {
		id[index] = ulidEncoding[time%32]
		time /= 32
	}
	copy(id[ulidTimeLength:], s.last[:])
	return string(id[:])
}

// incrementBase32 adds one to value in place and reports false when every
// character was already the last of the alphabet.
func incrementBase32(value []byte) bool {
	for index := len(value) - 1; index >= 0; index-- {
		position := strings.IndexByte(ulidEncoding, value[index])
		if position < len(ulidEncoding)-1 {
			value[index] = ulidEncoding[position+1]
			return true
		}
		value[index] = ulidEncoding[0]
	}
	return false
}

// hookTokenBatch is the number of bytes drawn for each hook token.
const hookTokenBatch = 34

// hookToken returns the next hook token.
func (s *runSeed) hookToken() string {
	var random [hookTokenBatch]byte
	_, _ = s.Read(random[:])
	token := make([]byte, 21)
	for index := range token {
		token[index] = hookTokenAlphabet[random[hookTokenBatch-1-index]&63]
	}
	return string(token)
}

// arc4Random is seedrandom's default generator: RC4 keyed by the seed's
// UTF-16 code units, with its first 256 outputs dropped, producing doubles
// with 52 random bits.
type arc4Random struct {
	i, j uint8
	s    [256]uint8
}

func (r *arc4Random) init(seed string) {
	units := utf16.Encode([]rune(seed))
	var key []int
	smear := 0
	for index, unit := range units {
		slot := index & 255
		if slot == len(key) {
			key = append(key, 0)
		}
		smear ^= key[slot] * 19
		key[slot] = (smear + int(unit)) & 255
	}
	if len(key) == 0 {
		key = []int{0}
	}
	for index := range r.s {
		r.s[index] = uint8(index)
	}
	j := 0
	for index := range r.s {
		t := r.s[index]
		j = (j + key[index%len(key)] + int(t)) & 255
		r.s[index], r.s[j] = r.s[j], t
	}
	r.output(256)
}

// output returns the next count outputs as one big-endian number.
func (r *arc4Random) output(count int) float64 {
	result := 0.0
	for ; count > 0; count-- {
		r.i++
		t := r.s[r.i]
		r.j += t
		r.s[r.i] = r.s[r.j]
		r.s[r.j] = t
		result = result*256 + float64(r.s[r.s[r.i]+t])
	}
	return result
}

// next returns a value in [0, 1).
func (r *arc4Random) next() float64 {
	const (
		significance = 1 << 52
		overflow     = 1 << 53
	)
	n, d, x := r.output(6), float64(1<<48), 0.0
	for n < significance {
		n = (n + x) * 256
		d *= 256
		x = r.output(1)
	}
	for n >= overflow {
		n /= 2
		d /= 2
		x = float64(uint32(x) >> 1)
	}
	return (n + x) / d
}
