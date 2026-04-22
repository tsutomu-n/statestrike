# SUPERPOWERS STATE
**Goal:** Implement the approved Phase 1.5 follow-up from `.plan/01next.md` by adding code support for mainnet public-data smoke runs over the existing Phase 1 pipeline, strengthening quality audit/export validation, and keeping live execution paths untouched.
**TestCommand:** `CI=true timeout 180 uv run --group dev --extra phase1-data --extra execution pytest tests/test_settings.py tests/test_collector.py tests/test_storage.py tests/test_quality.py tests/test_exports.py tests/test_smoke.py -q`
**AllowedFiles:** `.codex/SP_STATE.md`, `src/statestrike/settings.py`, `src/statestrike/collector.py`, `src/statestrike/storage.py`, `src/statestrike/quality.py`, `src/statestrike/exports.py`, `src/statestrike/smoke.py`, `tests/test_settings.py`, `tests/test_collector.py`, `tests/test_storage.py`, `tests/test_quality.py`, `tests/test_exports.py`, `tests/test_smoke.py`, `tests/fixtures/hyperliquid/candle.json`

## TASKS
- [x] TASK 1: [RED] Write failing tests for Phase 1.5 smoke configuration defaults, symbol allowlist guardrails, and collector handling for supplemental `candle` traffic without widening canonical normalized tables.
- [x] TASK 2: [GREEN] Implement the settings and collector changes needed for TASK 1 while keeping live-order scope disabled and Phase 1 canonical tables unchanged.
- [x] TASK 3: [RED] Write failing tests for richer quality/export validation outputs, including thresholdable skew stats, quarantine-rate inputs, and export bundle integrity summaries.
- [x] TASK 4: [GREEN] Implement the audit/export report changes needed for TASK 3 without adding new dependencies or execution-path code.
- [x] TASK 5: [RED] Write a failing integration test for a one-shot smoke pipeline runner that ingests captured public messages, writes raw/normalized/quarantine outputs, emits a manifest, and produces audit/export validation artifacts.
- [x] TASK 6: [GREEN] Implement `src/statestrike/smoke.py` and the minimal supporting storage/report wiring so TASK 5 passes for fixture-driven smoke batches and is ready to back a real mainnet run.
- [x] TASK 7: [REFACTOR] Run the target command, then `CI=true timeout 180 uv run --group dev --extra phase1-data --extra execution pytest -q`, and confirm the Phase 1.5 additions did not reopen any live execution path.
