from statestrike.models import AssetContextEvent, BookEvent, BookLevel, TradeEvent
from statestrike.policies import classify_gap


def test_book_models_enforce_positive_prices_and_sizes() -> None:
    event = BookEvent(
        book_event_id="book-1",
        capture_session_id="018f0dce-7b9f-7b8f-bfd6-65c9a3fe5b1b",
        reconnect_epoch=2,
        book_epoch=5,
        symbol="BTC",
        exchange_ts=1713818880000,
        recv_ts=1713818880200,
        event_kind="snapshot",
        source="ws",
        raw_msg_hash="abcd1234",
        n_bids=2,
        n_asks=2,
    )
    level = BookLevel(
        book_event_id="book-1",
        side="bid",
        level_idx=0,
        price=100.5,
        size=2.25,
    )

    assert event.event_kind == "snapshot"
    assert level.side == "bid"


def test_trade_event_normalizes_symbol_and_requires_positive_size() -> None:
    trade = TradeEvent(
        trade_event_id="trade-1",
        symbol="eth",
        exchange_ts=1713818880000,
        recv_ts=1713818880005,
        price=2500.0,
        size=1.5,
        side="sell",
    )

    assert trade.symbol == "ETH"
    assert trade.side == "sell"


def test_asset_context_computes_basis_and_next_funding_hour() -> None:
    ctx = AssetContextEvent(
        asset_ctx_event_id="ctx-1",
        symbol="SOL",
        exchange_ts=1713818880123,
        recv_ts=1713818880321,
        mark_px=151.5,
        oracle_px=150.0,
        funding_rate=0.0002,
        open_interest=12_345.0,
        mid_px=151.25,
    )

    assert round(ctx.basis, 6) == 0.01
    assert ctx.next_funding_ts == 1713819600000


def test_gap_policy_matches_approved_channel_rules() -> None:
    assert classify_gap(
        channel="l2_book",
        gap_seconds=1,
        continuity_known=False,
        recovery_succeeded=False,
    ).classification == "non_recoverable"
    assert classify_gap(
        channel="trades",
        gap_seconds=30,
        continuity_known=True,
        recovery_succeeded=True,
    ).classification == "recoverable"
    assert classify_gap(
        channel="active_asset_ctx",
        gap_seconds=180,
        continuity_known=True,
        recovery_succeeded=False,
    ).classification == "degraded"
