---
"go-workflow": minor
---

Store streams, run failure codes, and compressed payloads as the Workflow SDK does: a run's streams are named `strm_<ULID>_user`, run_failed events carry the SDK's `errorCode`, the Vercel World tags events with the delivery's `vercelId`, and payloads compress with zstd unless `WORKFLOW_COMPRESSION_CODEC=gzip`.
