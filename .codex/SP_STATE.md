# SUPERPOWERS STATE
**Goal:** Implement the approved Phase 1 StateStrike data-platform plan from `.plan/00next.md` on top of the existing bootstrap by adding fixture-driven market-data collection, raw/normalized/quarantine storage, Pandera schema gating, DuckDB quality audit, a Nautilus compare-ready export bundle, and an hftbacktest `.npz` export, while explicitly excluding live order-path and real network collection.
**TestCommand:** `CI=true timeout 180 uv run --group dev --extra phase1-data --extra execution pytest -q`
**AllowedFiles:** `pyproject.toml`, `uv.lock`, `.codex/SP_STATE.md`, `src/statestrike/settings.py`, `src/statestrike/models.py`, `src/statestrike/policies.py`, `src/statestrike/paths.py`, `src/statestrike/collector.py`, `src/statestrike/storage.py`, `src/statestrike/normalize.py`, `src/statestrike/schemas.py`, `src/statestrike/quality.py`, `src/statestrike/exports.py`, `tests/test_settings.py`, `tests/test_paths.py`, `tests/test_collector.py`, `tests/test_storage.py`, `tests/test_normalize.py`, `tests/test_schemas.py`, `tests/test_quality.py`, `tests/test_exports.py`, `tests/fixtures/hyperliquid/l2_book.json`, `tests/fixtures/hyperliquid/trades.json`, `tests/fixtures/hyperliquid/active_asset_ctx.json`

## TASKS
- [x] TASK 1: [RED] Write failing tests for phase-1 collector/storage settings, raw/quarantine path builders, and compressed raw manifest writing.
- [x] TASK 2: [GREEN] Implement settings/path/storage support so TASK 1 passes without widening scope.
- [x] TASK 3: [RED] Write failing tests for Hyperliquid fixture normalization and Pandera schema-gate quarantine splitting.
- [x] TASK 4: [GREEN] Implement `collector.py`, `normalize.py`, and `schemas.py` so TASK 3 passes using fixture replay only.
- [x] TASK 5: [RED] Write failing tests for DuckDB quality audit, Nautilus compare-ready export bundle, and hftbacktest `.npz` export.
- [x] TASK 6: [GREEN] Implement `quality.py`, `exports.py`, and any required model/policy extensions so TASK 5 passes.
- [x] TASK 7: [REFACTOR] Run the full test command, keep the implementation aligned with `.plan/00next.md`, and remove duplication or accidental scope growth.
