package vercelworld

import (
	"os/exec"
	"regexp"
	"strings"
	"testing"
)

// TestScriptsCompile compiles every script with LuaJIT (Lua 5.1, like Redis)
// and rejects globals Redis scripts cannot create or read.
func TestScriptsCompile(t *testing.T) {
	luajit, err := exec.LookPath("luajit")
	if err != nil {
		t.Skip("luajit is not installed")
	}
	allowed := map[string]bool{"redis": true, "cjson": true, "ARGV": true, "KEYS": true, "string": true, "table": true, "math": true, "tonumber": true, "tostring": true, "type": true, "ipairs": true, "pairs": true, "unpack": true, "next": true, "error": true}
	global := regexp.MustCompile(`(GGET|GSET).*; "([^"]+)"`)
	for name, script := range allScripts {
		cmd := exec.Command(luajit, "-bl", "-")
		cmd.Stdin = strings.NewReader(script.src)
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%s does not compile: %v\n%s", name, err, output)
		}
		for _, match := range global.FindAllStringSubmatch(string(output), -1) {
			if match[1] == "GSET" || !allowed[match[2]] {
				t.Errorf("%s uses global %s %q", name, match[1], match[2])
			}
		}
	}
}

func TestScriptsHaveDistinctDigests(t *testing.T) {
	seen := map[string]string{}
	for name, script := range allScripts {
		if len(script.sha) != 40 {
			t.Fatalf("%s sha = %q", name, script.sha)
		}
		if other, ok := seen[script.sha]; ok {
			t.Fatalf("%s and %s share a digest", name, other)
		}
		seen[script.sha] = name
	}
}
