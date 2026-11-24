package sdkdrift

import (
	"slices"
	"testing"
)

func TestTestNamesReadsNestedCallsAndModifiers(t *testing.T) {
	source := `
import { describe, it, test } from 'vitest';
// it('commented out', () => {});
describe('outer', () => {
  it('first', () => {
    expect('{').toBe("}");
  });
  describe.skipIf(process.env.X)("inner", () => {
    it.each([
      [1, 'a'],
      [2, 'b'],
    ])('runs %s', (n) => {});
    test(
      'named on the next line',
      async () => {}
    );
  });
  it.concurrent(` + "`template ${1 + 1}`" + `, () => {});
});
test('top level', () => {});
it(variableName, () => {});
`
	want := []string{
		"outer > first",
		"outer > inner > runs %s",
		"outer > inner > named on the next line",
		"outer > template ${1 + 1}",
		"top level",
	}
	if got := TestNames(source); !slices.Equal(got, want) {
		t.Fatalf("TestNames = %q, want %q", got, want)
	}
}
