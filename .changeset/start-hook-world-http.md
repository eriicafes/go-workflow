---
"go-workflow": minor
---

world-http advertises `atomicStartHook` when the Go World creates start hooks, passes `startHook` through, and reports a refused one as the SDK's `HookConflictError`.
