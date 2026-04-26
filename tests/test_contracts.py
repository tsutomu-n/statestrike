from __future__ import annotations

from pathlib import Path


def test_contracts_document_freezes_core_substrate_terms() -> None:
    text = Path("docs/CONTRACTS.md").read_text(encoding="utf-8")

    required_terms = [
        "truth capture artifact = `capture_log`",
        "derived capture artifact = `raw_ws`",
        "fixture-only fallback = `recv_ts_start`",
        "network truth path requires ingress metadata",
        "ordering key = `recv_ts_ns` + `recv_seq`",
        "`recv_ts` is a legacy/reporting field",
        "`substrate_ready`",
        "`nautilus_baseline_ready`",
        "`nautilus_baseline_candidate`",
        "`funding_aware_ready`",
        "`enrichment_source`",
        "`enrichment_asof_ts`",
        "`enrichment_kind`",
        "`raw_root`",
        "`raw_paths`",
        "`build_raw_path()`",
    ]
    for term in required_terms:
        assert term in text


def test_contracts_document_freezes_storage_and_query_roles() -> None:
    text = Path("docs/CONTRACTS.md").read_text(encoding="utf-8")

    required_terms = [
        "parquet segment writer = primary sink",
        "DuckDB = query / analytics engine",
        "DuckDB may remain in readiness, quality, and diagnostics backtests",
        "New write paths must not depend on DuckDB",
    ]
    for term in required_terms:
        assert term in text
