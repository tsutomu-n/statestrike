# SUPERPOWERS STATE
**Goal:** Persist Phase 1.5 smoke campaign summaries to stable report files so longer-running mainnet smoke campaigns leave an auditable on-disk record without touching any execution/live-order path.
**TestCommand:** `CI=true timeout 180 uv run --group dev --extra phase1-data --extra execution pytest tests/test_paths.py tests/test_smoke.py -q`
**AllowedFiles:** `.codex/SP_STATE.md`, `src/statestrike/paths.py`, `src/statestrike/smoke.py`, `tests/test_paths.py`, `tests/test_smoke.py`

## TASKS
- [x] TASK 1: [RED] Add failing tests for a stable campaign report path and for campaign runs that must persist JSON/Markdown summaries to disk.
- [x] TASK 2: [GREEN] Implement the campaign report path builder and on-disk summary persistence without widening scope beyond Phase 1.5 smoke reporting.
- [x] TASK 3: [RED] Add a failing CLI test that requires campaign-mode stdout JSON to expose the persisted report paths.
- [x] TASK 4: [GREEN] Implement the CLI/report plumbing so TASK 3 passes while preserving existing one-shot behavior.
- [x] TASK 5: [REFACTOR] Run the target test command, then `CI=true timeout 180 uv run --group dev --extra phase1-data --extra execution pytest -q`, then execute a short mainnet proof run in campaign mode and confirm the persisted summary report exists and matches the JSON output.
