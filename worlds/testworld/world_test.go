package testworld_test

import (
	"testing"

	"github.com/eriicafes/go-workflow/workflowtest"
	"github.com/eriicafes/go-workflow/worlds"
	"github.com/eriicafes/go-workflow/worlds/testworld"
)

func TestConformance(t *testing.T) {
	t.Parallel()
	workflowtest.Conformance(t, func(t *testing.T) worlds.World {
		world := testworld.New()
		t.Cleanup(world.Close)
		return world
	})
}

func TestConformanceShared(t *testing.T) {
	t.Parallel()
	workflowtest.ConformanceShared(t, func(t *testing.T) (worlds.World, worlds.World) {
		world := testworld.New()
		t.Cleanup(world.Close)
		return world, world
	})
}
