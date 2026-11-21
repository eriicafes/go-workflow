---
"go-workflow": minor
---

`workflow.SetAttributes(ctx, attributes, options...)` changes a run's attributes as one `attr_set` event, with changes in key order and a nil value deleting its key. In workflow context the write draws the ID `attr_<ULID>` from the run's seed, is recorded in-band with a `workflow` writer, and replay matches it by ID. In step context it is recorded out-of-band with a `step` writer naming the step and attempt. `worlds.DataAttrSet` and `RunsSetAttributesInput` gain `AllowReservedAttributes`, and `worlds.DurableSetAttributes` records a workflow write.
