# SUPERPOWERS STATE
**Goal:** Persist Phase 1.5 smoke campaign progress incrementally and on failure so longer-running mainnet smoke campaigns leave a usable partial audit trail without touching any execution/live-order path.
**TestCommand:** `CI=true timeout 180 uv run --group dev --extra phase1-data --extra execution pytest tests/test_smoke.py -q`
**AllowedFiles:** `.codex/SP_STATE.md`, `src/statestrike/smoke.py`, `tests/test_smoke.py`

## TASKS
- [x] TASK 1: [RED] Add failing tests that require campaign summaries to record progress after each completed session and to persist a failed state when a later session crashes.
- [x] TASK 2: [GREEN] Implement incremental/failure summary persistence in `src/statestrike/smoke.py` without widening scope beyond Phase 1.5 smoke orchestration.
- [x] TASK 3: [RED] Add a failing CLI test that requires campaign-mode failures to leave a persisted failed summary on disk.
- [x] TASK 4: [GREEN] Implement the CLI-compatible failure persistence so TASK 3 passes while preserving existing success-path behavior.
- [x] TASK 5: [REFACTOR] Run the target test command, then `CI=true timeout 180 uv run --group dev --extra phase1-data --extra execution pytest -q`, then execute a short failure-style proof run to confirm the persisted summary ends in `failed` status with partial session progress.
