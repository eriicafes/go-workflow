package sdkdrift

import (
	"strings"
	"unicode"
)

// TestNames returns the full names of the tests a TypeScript test file
// declares with test or it, inside any describe blocks, as "<describe> >
// ... > <test>". It reads the calls whose name is a string literal, after
// modifiers such as .each(table), .skipIf(condition) or .concurrent, and
// tracks braces, outside strings and comments, to know which describe blocks
// a test is in.
func TestNames(source string) []string {
	type block struct {
		name  string
		depth int
	}
	var (
		names  []string
		blocks []block
		depth  int
	)
	s := scanner{src: source}
	for s.pos < len(s.src) {
		c := s.src[s.pos]
		switch {
		case s.skipCommentOrString():
		case c == '{':
			depth++
			s.pos++
		case c == '}':
			depth--
			s.pos++
			for len(blocks) > 0 && blocks[len(blocks)-1].depth >= depth {
				blocks = blocks[:len(blocks)-1]
			}
		case isIdentStart(c) && (s.pos == 0 || !isIdentPart(s.src[s.pos-1]) && s.src[s.pos-1] != '.'):
			start := s.pos
			for s.pos < len(s.src) && isIdentPart(s.src[s.pos]) {
				s.pos++
			}
			word := s.src[start:s.pos]
			if word != "describe" && word != "test" && word != "it" {
				continue
			}
			name, ok := s.callName()
			if !ok {
				continue
			}
			if word == "describe" {
				blocks = append(blocks, block{name: name, depth: depth})
				continue
			}
			full := make([]string, 0, len(blocks)+1)
			for _, b := range blocks {
				full = append(full, b.name)
			}
			names = append(names, strings.Join(append(full, name), " > "))
		default:
			s.pos++
		}
	}
	return names
}

type scanner struct {
	src string
	pos int
}

// skipCommentOrString skips the comment or string literal at pos, and
// reports whether there was one.
func (s *scanner) skipCommentOrString() bool {
	rest := s.src[s.pos:]
	switch {
	case strings.HasPrefix(rest, "//"):
		if end := strings.IndexByte(rest, '\n'); end >= 0 {
			s.pos += end
		} else {
			s.pos = len(s.src)
		}
		return true
	case strings.HasPrefix(rest, "/*"):
		if end := strings.Index(rest[2:], "*/"); end >= 0 {
			s.pos += end + 4
		} else {
			s.pos = len(s.src)
		}
		return true
	case rest[0] == '\'' || rest[0] == '"' || rest[0] == '`':
		s.string()
		return true
	}
	return false
}

// string reads the string literal at pos and returns its text, with
// escapes resolved for ' and " strings and template literals kept as
// written.
func (s *scanner) string() string {
	quote := s.src[s.pos]
	s.pos++
	var b strings.Builder
	for s.pos < len(s.src) {
		c := s.src[s.pos]
		switch {
		case c == '\\' && s.pos+1 < len(s.src):
			b.WriteByte(s.src[s.pos+1])
			s.pos += 2
		case c == quote:
			s.pos++
			return b.String()
		case quote == '`' && c == '$' && s.pos+1 < len(s.src) && s.src[s.pos+1] == '{':
			// An interpolation, kept as written up to its closing brace.
			open := 0
			for s.pos < len(s.src) {
				ch := s.src[s.pos]
				b.WriteByte(ch)
				s.pos++
				if ch == '{' {
					open++
				} else if ch == '}' {
					if open--; open == 0 {
						break
					}
				}
			}
		default:
			b.WriteByte(c)
			s.pos++
		}
	}
	return b.String()
}

// callName reads the rest of a describe, test or it call after its keyword:
// modifiers such as .each(table) or .skipIf(condition), then the call's
// opening parenthesis and its first argument. It returns that argument when
// it is a string literal.
func (s *scanner) callName() (string, bool) {
	for {
		s.space()
		if s.pos >= len(s.src) {
			return "", false
		}
		switch s.src[s.pos] {
		case '.':
			s.pos++
			start := s.pos
			for s.pos < len(s.src) && isIdentPart(s.src[s.pos]) {
				s.pos++
			}
			switch s.src[start:s.pos] {
			case "each", "for", "skipIf", "runIf":
				// A modifier with arguments, such as each's table. The call
				// that names the test follows them.
				s.space()
				if s.pos >= len(s.src) || s.src[s.pos] != '(' || !s.skipParens() {
					return "", false
				}
			}
		case '(':
			s.pos++
			s.space()
			if s.pos < len(s.src) && (s.src[s.pos] == '\'' || s.src[s.pos] == '"' || s.src[s.pos] == '`') {
				return s.string(), true
			}
			return "", false
		default:
			return "", false
		}
	}
}

// skipParens skips the balanced parentheses at pos, with strings and
// comments in them, and reports whether they closed.
func (s *scanner) skipParens() bool {
	open := 0
	for s.pos < len(s.src) {
		if s.skipCommentOrString() {
			continue
		}
		switch s.src[s.pos] {
		case '(':
			open++
		case ')':
			if open--; open == 0 {
				s.pos++
				return true
			}
		}
		s.pos++
	}
	return false
}

func (s *scanner) space() {
	for s.pos < len(s.src) && unicode.IsSpace(rune(s.src[s.pos])) {
		s.pos++
	}
}

func isIdentStart(c byte) bool {
	return c == '_' || c == '$' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isIdentPart(c byte) bool { return isIdentStart(c) || c >= '0' && c <= '9' }
