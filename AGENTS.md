# Repository Guidelines

## Project Structure & Module Organization
This repository is a small library-first Python package with a `src/` layout. Authoritative code lives in `src/statestrike/`: `models.py` holds frozen Pydantic domain models, `settings.py` handles `.env`-backed configuration, `identifiers.py` contains deterministic IDs and hashing, `paths.py` builds partitioned storage paths, and `policies.py` defines gap-recovery rules. Tests live in `tests/` as `test_<module>.py`. Root `0422*.md` files are design references; implementation truth should stay in code and tests. Do not hand-edit generated caches such as `__pycache__/` or build output in `dist/`.

## Build, Test, and Development Commands
Use `uv` and Python 3.13 only (`>=3.13,<3.14`).

- `uv sync --group dev`: install base runtime and test dependencies.
- `uv sync --group dev --extra phase1-data`: add DuckDB, Pandera, and market-data extras.
- `uv sync --group dev --extra execution`: add execution-stack extras.
- `uv run pytest`: run the full test suite.
- `uv run pytest tests/test_models.py -q`: run one focused test file.
- `uv build`: produce the wheel and sdist with Hatchling.

## Coding Style & Naming Conventions
Follow the existing style: 4-space indentation, explicit type hints, and `from __future__ import annotations`. Use `snake_case` for functions, modules, and variables; use `PascalCase` for models such as `BookEvent` and `GapDecision`. Prefer small pure functions and immutable Pydantic models when the current module already uses `ConfigDict(frozen=True)`. If new logging is introduced, use `loguru` rather than the standard `logging` module and keep logger setup consistent within the touched module. No formatter or linter is configured yet, so keep imports tidy and stay close to PEP 8.

## Testing Guidelines
Tests use `pytest` and are currently fast, deterministic unit tests. Add or update tests for validation boundaries, symbol uppercasing, deterministic identifiers, partition path shapes, and phase-1 safety constraints. Keep names direct: `test_<behavior>()`. Prefer `tmp_path` for file-backed settings tests and avoid network-dependent coverage.

## Commit & Pull Request Guidelines
Visible history currently uses Conventional Commit style, for example `feat: bootstrap statestrike phase1 data platform`; continue with `feat:`, `fix:`, `test:`, or `docs:` plus a short imperative summary. PRs should state the problem, the exact change, and the verification performed (`uv run pytest`, `uv build`). Call out any `.env` or dependency-extra changes explicitly.

## Security & Configuration Notes
Configuration is loaded from `.env` with the `STATESTRIKE_` prefix. Never commit secrets or production credentials. Preserve the current phase-1 guardrail: live orders must remain disabled (`STATESTRIKE_ENABLE_LIVE_ORDERS=false`). If you add optional integrations, keep them behind extras rather than expanding the base dependency set.
