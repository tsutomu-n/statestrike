# SUPERPOWERS STATE
**Goal:** Bootstrap the Phase 1 StateStrike data-platform codebase from the approved docs by adding a pinned Python project, core settings/contracts, deterministic identifiers, normalized market-data models, channel-specific gap policies, and export path conventions with tests, while explicitly excluding live order-path and networked collector work.
**TestCommand:** `CI=true timeout 120 uv run pytest -q`
**AllowedFiles:** `pyproject.toml`, `uv.lock`, `src/statestrike/__init__.py`, `src/statestrike/settings.py`, `src/statestrike/identifiers.py`, `src/statestrike/models.py`, `src/statestrike/policies.py`, `src/statestrike/paths.py`, `tests/test_settings.py`, `tests/test_identifiers.py`, `tests/test_models.py`, `tests/test_paths.py`

## TASKS
- [x] TASK 1: [RED] Write failing tests for exact-pinned settings loading, release channels, mainnet market-data defaults, and phase-1 scope defaults encoded from `回答.md`.
- [x] TASK 2: [GREEN] Implement `pyproject.toml`, package bootstrap, and `settings.py` so TASK 1 passes.
- [x] TASK 3: [RED] Write failing tests for deterministic identifiers and canonical dedup hashing rules.
- [x] TASK 4: [GREEN] Implement `identifiers.py` so TASK 3 passes.
- [x] TASK 5: [RED] Write failing tests for `BookEvent`, `BookLevel`, `TradeEvent`, `AssetContextEvent`, channel-specific gap policies, and export path conventions required by phase 1.
- [x] TASK 6: [GREEN] Implement `models.py`, `policies.py`, and `paths.py` so TASK 5 passes.
- [x] TASK 7: [REFACTOR] Run the full test command, keep the API consistent with the approved docs, and clean up naming or duplication without widening scope.
