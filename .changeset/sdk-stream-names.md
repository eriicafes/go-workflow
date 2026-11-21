---
"go-workflow": minor
---

Name a run's streams `strm_<ULID>_user` and `strm_<ULID>_user_<base64url name>`, record `errorCode` on `run_failed` and `vercelId` on Vercel events, and compress payloads with zstd, or gzip with `WORKFLOW_COMPRESSION_CODEC=gzip`.
