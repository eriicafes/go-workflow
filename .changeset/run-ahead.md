---
"go-workflow": minor
---

Let a flow hand an inline step's result to the runner before its write commits, so consecutive inline steps overlap their writes, wherever no other writer's event could change a decision. `core.WithRunAheadDepth` sets how many results may be unconfirmed, 10 by default and 0 for none.
