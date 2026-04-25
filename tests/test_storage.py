from __future__ import annotations

import json
from datetime import date

import zstandard

from statestrike.models import ManifestRecord
from statestrike.recovery import MessageCaptureContext, MessageIngressMeta
from statestrike.storage import (
    CaptureLogWriter,
    NormalizedWriter,
    QuarantineWriter,
    RawWriter,
)


def test_raw_writer_writes_compressed_jsonl_and_manifest(tmp_path) -> None:
    writer = RawWriter(root=tmp_path)
    trading_date = date(2026, 4, 22)
    capture_session_id = "018f0dce-7b9f-7b8f-bfd6-65c9a3fe5b1b"
    messages = [
        {
            "channel": "trades",
            "data": [{"coin": "BTC", "side": "B", "px": "100.0", "sz": "0.5", "time": 1}],
        }
    ]

    raw_path = writer.write_batch(
        trading_date=trading_date,
        channel="trades",
        symbol="BTC",
        capture_session_id=capture_session_id,
        batch_id="0001",
        messages=messages,
    )

    with zstandard.open(raw_path, "rt", encoding="utf-8") as handle:
        lines = [json.loads(line) for line in handle]

    assert lines == messages

    manifest = ManifestRecord(
        capture_session_id=capture_session_id,
        started_at="2026-04-22T00:00:00Z",
        ended_at="2026-04-22T00:00:01Z",
        channels=("trades",),
        symbols=("BTC",),
        row_count=1,
        ws_disconnect_count=0,
        reconnect_count=0,
        gap_flags=(),
    )
    manifest_path = writer.write_manifest(
        trading_date=trading_date,
        manifest=manifest,
    )

    assert manifest_path.name == f"{capture_session_id}.json"
    manifest_json = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest_json["row_count"] == 1
    assert manifest_json["truth_capture_artifact"] == "capture_log"
    assert manifest_json["corrected_export_targets"] == ["hftbacktest"]


def test_capture_log_writer_writes_session_global_ordered_entries(tmp_path) -> None:
    writer = CaptureLogWriter(root=tmp_path)
    path = writer.write_batch(
        trading_date=date(2026, 4, 22),
        capture_session_id="session-1",
        batch_id="0001",
        messages=[
            {"channel": "trades", "data": [{"coin": "BTC"}]},
            {"channel": "l2Book", "data": {"coin": "BTC"}},
        ],
        ingress_metadata=[
            MessageIngressMeta(
                recv_wall_ns=1713818880100000000,
                recv_mono_ns=100,
                recv_seq=0,
                connection_id="conn-0",
            ),
            MessageIngressMeta(
                recv_wall_ns=1713818880101000000,
                recv_mono_ns=101,
                recv_seq=1,
                connection_id="conn-0",
            ),
        ],
        message_contexts=[
            MessageCaptureContext(reconnect_epoch=0, book_epoch=1),
            MessageCaptureContext(
                reconnect_epoch=0,
                book_epoch=1,
                book_event_kind="snapshot",
            ),
        ],
    )

    with zstandard.open(path, "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle]

    assert path.name == "capture-log-0001.jsonl.zst"
    assert rows[0]["message"]["channel"] == "trades"
    assert rows[0]["ingress"]["recv_seq"] == 0
    assert rows[1]["message"]["channel"] == "l2Book"
    assert rows[1]["message_context"]["book_event_kind"] == "snapshot"


def test_normalized_and_quarantine_writers_create_partitioned_parquet(tmp_path) -> None:
    normalized = NormalizedWriter(root=tmp_path)
    quarantine = QuarantineWriter(root=tmp_path)
    trading_date = date(2026, 4, 22)

    normalized_path = normalized.write_rows(
        table="trades",
        trading_date=trading_date,
        symbol="BTC",
        rows=[
            {
                "trade_event_id": 1,
                "symbol": "BTC",
                "exchange_ts": 1,
                "recv_ts": 2,
                "price": 100.0,
                "size": 0.5,
                "side": "buy",
                "capture_session_id": "session-1",
                "source": "ws",
                "raw_msg_hash": "raw-1",
                "dedup_hash": "dedup-1",
            }
        ],
    )
    quarantine_path = quarantine.write_rows(
        table="trades",
        trading_date=trading_date,
        symbol="BTC",
        rows=[
            {
                "trade_event_id": 2,
                "symbol": "BTC",
                "exchange_ts": 1,
                "recv_ts": 2,
                "price": 0.0,
                "size": 0.5,
                "side": "buy",
                "capture_session_id": "session-1",
                "source": "ws",
                "raw_msg_hash": "raw-2",
                "dedup_hash": "dedup-2",
            }
        ],
    )

    assert normalized_path.suffix == ".parquet"
    assert quarantine_path.suffix == ".parquet"
    assert normalized_path.name.startswith("part-")
    assert quarantine_path.name.startswith("part-")
    assert normalized_path.name != "part-0001.parquet"
    assert quarantine_path.name != "part-0001.parquet"


def test_normalized_writer_uses_non_sequential_unique_part_filenames(tmp_path) -> None:
    normalized = NormalizedWriter(root=tmp_path)
    trading_date = date(2026, 4, 22)
    rows = [
        {
            "trade_event_id": "trade-1",
            "native_tid": "1",
            "symbol": "BTC",
            "exchange_ts": 1,
            "recv_ts": 2,
            "price": 100.0,
            "size": 0.5,
            "side": "buy",
            "capture_session_id": "session-1",
            "reconnect_epoch": 0,
            "source": "ws",
            "raw_msg_hash": "raw-1",
            "dedup_hash": "dedup-1",
        }
    ]

    first_path = normalized.write_rows(
        table="trades",
        trading_date=trading_date,
        symbol="BTC",
        rows=rows,
    )
    second_path = normalized.write_rows(
        table="trades",
        trading_date=trading_date,
        symbol="BTC",
        rows=[
            {
                **rows[0],
                "trade_event_id": "trade-2",
                "native_tid": "2",
                "dedup_hash": "dedup-2",
            }
        ],
    )

    assert first_path != second_path
    assert first_path.exists()
    assert second_path.exists()
    assert first_path.name.startswith("part-")
    assert second_path.name.startswith("part-")
    assert first_path.name != "part-0001.parquet"
    assert second_path.name != "part-0002.parquet"
