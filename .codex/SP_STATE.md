# SUPERPOWERS STATE
**Goal:** Make the Phase 1.5 audit thresholds explicit and configurable through settings and the smoke CLI, then persist those thresholds in the quality reports without touching any execution/live-order path.
**TestCommand:** `CI=true timeout 180 uv run --group dev --extra phase1-data --extra execution pytest tests/test_settings.py tests/test_quality.py tests/test_smoke.py -q`
**AllowedFiles:** `.codex/SP_STATE.md`, `src/statestrike/settings.py`, `src/statestrike/quality.py`, `src/statestrike/smoke.py`, `tests/test_settings.py`, `tests/test_quality.py`, `tests/test_smoke.py`

## TASKS
- [x] TASK 1: [RED] Add failing tests for the new Phase 1.5 audit threshold settings defaults, env overrides, and invalid threshold ordering.
- [x] TASK 2: [GREEN] Implement the threshold settings surface in `src/statestrike/settings.py` without widening scope beyond smoke/audit runtime behavior.
- [x] TASK 3: [RED] Add failing tests that require `QualityAuditReport` and smoke artifacts to persist the active threshold values in JSON/Markdown/CLI output.
- [x] TASK 4: [GREEN] Implement the quality-report and smoke-CLI plumbing so TASK 3 passes while keeping existing export/runtime behavior intact.
- [x] TASK 5: [REFACTOR] Run the target test command, then `CI=true timeout 180 uv run --group dev --extra phase1-data --extra execution pytest -q`, then execute one short mainnet proof run to confirm the emitted audit report includes the configured thresholds.
