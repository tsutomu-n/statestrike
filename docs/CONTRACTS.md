# StateStrike Contracts

This document freezes the current substrate contracts after the timestamp,
readiness-profile, and funding-enrichment lifts. It is the implementation-facing
contract for tests, exports, and future bridges.

## Capture Artifacts

- truth capture artifact = `capture_log`
- derived capture artifact = `raw_ws`

`capture_log` is the session-global ordered parsed capture artifact. It stores
the received message, per-message ingress metadata, and message recovery context.
It is not the original websocket byte stream.

`raw_ws` is a derived per-channel capture artifact partitioned for inspection and
compatibility. It must not be described as truth raw capture.

## Fixture Fallback

- fixture-only fallback = `recv_ts_start`
- network truth path requires ingress metadata

`recv_ts_start` exists only to keep fixture replay deterministic. Network capture
must provide ingress metadata for every message. Missing ingress metadata in the
network path is invalid; it must not be replaced with synthetic receive times.

## Ordering

- ordering key = `recv_ts_ns` + `recv_seq`
- `recv_ts` is a legacy/reporting field

Canonical rows carry `recv_ts_ns` and `recv_seq`. `recv_ts_ns` is the wall-clock
receive timestamp in nanoseconds, and `recv_seq` is the stable per-connection
arrival sequence. `recv_ts` remains available as millisecond reporting data and
must not be used as the canonical ordering key.

For local-arrival ordering, use `(recv_ts_ns, recv_seq)`. For exchange-order
views, use `(exchange_ts, recv_ts_ns, recv_seq)`. Truth exports preserve
`ts_init_ns` from `recv_ts_ns`; do not derive `ts_init_ns` from millisecond
`recv_ts`.

## Readiness Profiles

Readiness is profile-aware:

- `substrate_ready`: normalized substrate completeness without requiring funding.
- `nautilus_baseline_ready`: truth/corrected export completeness for baseline
  crossing; funding gaps are warnings when the baseline ignores funding.
- `nautilus_baseline_candidate`: derived Nautilus-only baseline input that keeps
  strict `ready` semantics intact while allowing only explicitly approved
  warnings. Session-boundary replay duplicates require
  `baseline_input_manifest.session_replay_dedup_applied = true`.
- `funding_aware_ready`: full funding-aware readiness; missing funding enrichment
  is blocking.

Profile checks may require normalized tables, export artifacts, row counts,
capture logs, duplicate/gap thresholds, and funding enrichment depending on the
profile.

## Funding Enrichment

Funding enrichment is stored as a sidecar and does not mutate truth rows.
Current predicted-funding provenance fields are:

- `next_funding_ts`
- `funding_interval_hours`
- `enrichment_source`
- `enrichment_asof_ts`
- `enrichment_kind`

`enrichment_source` identifies the upstream source, currently
`hyperliquid_predictedFundings`. `enrichment_asof_ts` is the millisecond wall time
at which the enrichment was produced. `enrichment_kind` distinguishes
`predicted_funding`, future `history_derived`, and unsupported/manual cases.

Hyperliquid `predictedFundings` is first-perp-dex only. Unsupported dex requests
must be recorded as unsupported provenance and surfaced by readiness as
`funding_source_unsupported_for_dex`; they must not silently fall back.

## Storage And Query Roles

- parquet segment writer = primary sink
- DuckDB = query / analytics engine

The online write path persists capture logs, derived capture artifacts,
normalized rows, quarantine rows, and enrichment sidecars as files. Parquet
segments are written directly by the parquet segment writer; DuckDB is not the
primary sink for those writes.

DuckDB may remain in readiness, quality, and diagnostics backtests because those
paths run analytical queries over immutable parquet files.

New write paths must not depend on DuckDB. If a future module uses DuckDB, it
must stay on the query or analytics side of the boundary unless this contract is
deliberately revised.

## Legacy Aliases

The following names are legacy aliases retained for compatibility only:

- `raw_root`
- `raw_paths`
- `build_raw_path()`

New code should prefer `derived_capture_root`, `derived_capture_paths`, and
`build_derived_capture_path()`. Do not add new legacy aliases for capture
artifacts.
