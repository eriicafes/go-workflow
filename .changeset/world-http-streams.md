---
"go-workflow": patch
---

Fix `world-http` stream reads stalling before a stream has chunks, and `getChunks` pagination never ending.
