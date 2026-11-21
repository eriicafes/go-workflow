---
"go-workflow": minor
---

Add `workflow.SetAttributes`, which records an `attr_set` event from workflow or step code. A nil value deletes its key. `worlds.DataAttrSet` and `RunsSetAttributesInput` gain `AllowReservedAttributes`.
