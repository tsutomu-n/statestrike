# SUPERPOWERS STATE
**Goal:** Add a Phase 1.5 multi-session smoke campaign runner on top of the existing one-shot session path so the CLI can drive repeated public-data sessions safely for longer mainnet operation without touching any execution/live-order path.
**TestCommand:** `CI=true timeout 180 uv run --group dev --extra phase1-data --extra execution pytest tests/test_smoke.py -q`
**AllowedFiles:** `.codex/SP_STATE.md`, `src/statestrike/smoke.py`, `tests/test_smoke.py`

## TASKS
- [x] TASK 1: [RED] Add failing tests for a multi-session smoke campaign runner that reuses the existing session transport, emits unique session IDs/manifests, and accumulates daily artifacts across repeated sessions.
- [x] TASK 2: [GREEN] Implement the campaign runner in `src/statestrike/smoke.py` without widening scope beyond Phase 1.5 public-data smoke orchestration.
- [x] TASK 3: [RED] Add a failing test for a CLI contract that can invoke the campaign runner, print a JSON campaign summary, and preserve the existing single-session behavior.
- [x] TASK 4: [GREEN] Implement the CLI campaign mode so TASK 3 passes while keeping the current one-shot smoke path intact.
- [x] TASK 5: [REFACTOR] Run the target test command, then `CI=true timeout 180 uv run --group dev --extra phase1-data --extra execution pytest -q`, then execute a short mainnet proof run in campaign mode and confirm repeated sessions emit separate manifests plus a final JSON summary.
