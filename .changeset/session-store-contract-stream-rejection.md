---
"@routedock/routedock": minor
---

Add required `channel_contract` and `network` fields to `SessionState` and introduce `SessionStore.setStatus()` to update session status and settlement hashes without altering `cumulative_amount`. Fix unhandled promise rejections during pipelined streaming in `MppSessionClient.stream()`.
