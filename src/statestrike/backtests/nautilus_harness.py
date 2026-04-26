from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Literal

from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.config import BacktestDataConfig, BacktestRunConfig, BacktestVenueConfig
from nautilus_trader.model.currencies import BTC, USD
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.instruments import CryptoPerpetual
from nautilus_trader.model.objects import Money, Price, Quantity
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from pydantic import BaseModel, ConfigDict, Field

from statestrike.backtests import BacktestRunResult, run_baseline_simple_momentum
from statestrike.backtests.nautilus import _load_truth_trade_ticks
from statestrike.readiness import (
    BacktestReadinessReport,
    ReadinessProfileName,
    run_backtest_readiness,
)


class NautilusHarnessConfig(BaseModel):
    model_config = ConfigDict(frozen=True)

    assumed_taker_fee_rate: float = Field(default=0.0004, ge=0.0)
    assumed_maker_fee_rate: float = Field(default=0.0004, ge=0.0)
    fee_assumption_source: str = "project_fixed_research_assumption_not_exchange_live_tier"
    fill_model_name: str = "project_taker_fill_v1"
    slippage_assumption: float = Field(default=0.0, ge=0.0)
    funding_treatment: Literal["ignored"] = "ignored"
    max_open_positions: int = Field(default=1, ge=1)
    pyramiding: bool = False
    position_size_mode: Literal["fixed_notional"] = "fixed_notional"
    fixed_notional_usd: float = Field(default=1000.0, gt=0.0)


class NautilusHarnessMetrics(BaseModel):
    model_config = ConfigDict(frozen=True)

    gross_pnl: float
    fee_cost: float
    funding_pnl: float
    net_pnl: float
    trade_count: int = Field(ge=0)
    order_count: int = Field(ge=0)
    turnover: float = Field(ge=0.0)
    max_drawdown: float = Field(ge=0.0)
    avg_holding_time: float = Field(ge=0.0)
    exposure_time_ratio: float = Field(ge=0.0)
    ts_event_min_ns: int | None = None
    ts_event_max_ns: int | None = None
    ts_init_min_ns: int | None = None
    ts_init_max_ns: int | None = None
    equity_curve: tuple[float, ...]
    equity_curve_source: str = "closed_trade_net_pnl"


class NautilusHighLevelPathStatus(BaseModel):
    model_config = ConfigDict(frozen=True)

    status: Literal["configured"]
    catalog_path: str
    catalog_written: bool
    backtest_node_configured: bool
    components: tuple[str, ...]
    note: str


class NautilusHarnessParity(BaseModel):
    model_config = ConfigDict(frozen=True)

    same_symbol: bool
    same_input_trade_universe: bool
    exact_pnl_match_required: bool = False
    exact_fill_count_match_required: bool = False
    explanation: str


class NautilusHarnessResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_name: str
    symbol: str
    date_range: tuple[str, str]
    dataset_profile: str
    readiness_status: str
    config: NautilusHarnessConfig
    metrics: NautilusHarnessMetrics
    high_level_path: NautilusHighLevelPathStatus
    control: BacktestRunResult
    parity: NautilusHarnessParity
    readiness_report: BacktestReadinessReport


def run_nautilus_baseline_harness_v1(
    *,
    root: Path,
    trading_date: date,
    symbol: str,
    profile: ReadinessProfileName = "nautilus_baseline_candidate",
    config: NautilusHarnessConfig | None = None,
) -> NautilusHarnessResult:
    symbol = symbol.upper()
    config = config or NautilusHarnessConfig()
    readiness_report = run_backtest_readiness(
        root=root,
        trading_date=trading_date,
        symbols=(symbol,),
        profile=profile,
    )
    if readiness_report.status != "ready" or readiness_report.warning_reasons:
        reasons = readiness_report.blocking_reasons or readiness_report.warning_reasons
        raise ValueError(
            "Nautilus baseline harness requires warning-free ready candidate: "
            + ", ".join(reasons)
        )

    ticks = _load_truth_trade_ticks(
        root=root,
        trading_date=trading_date,
        symbol=symbol,
    )
    if len(ticks) < 3:
        raise ValueError("nautilus_baseline_harness_v1 requires at least 3 trade ticks")

    control = run_baseline_simple_momentum(
        root=root,
        trading_date=trading_date,
        symbol=symbol,
        profile=profile,
        allow_warning=False,
    )
    metrics = _run_target_simple_momentum(ticks=ticks, config=config)
    high_level_path = _configure_high_level_path(
        root=root,
        trading_date=trading_date,
        symbol=symbol,
        ticks=ticks,
    )
    parity = NautilusHarnessParity(
        same_symbol=control.symbol == symbol,
        same_input_trade_universe=control.trade_count == metrics.trade_count,
        explanation=(
            "Control uses in-repo normalized trades without execution costs; target "
            "uses the same Nautilus TradeTick universe with fixed taker-fee and "
            "position-sizing assumptions. Exact PnL and fill count are intentionally "
            "not parity requirements."
        ),
    )
    return NautilusHarnessResult(
        strategy_name="nautilus_baseline_harness_v1",
        symbol=symbol,
        date_range=(trading_date.isoformat(), trading_date.isoformat()),
        dataset_profile=readiness_report.profile.name,
        readiness_status=readiness_report.status,
        config=config,
        metrics=metrics,
        high_level_path=high_level_path,
        control=control,
        parity=parity,
        readiness_report=readiness_report,
    )


def compute_max_drawdown(equity_curve: tuple[float, ...]) -> float:
    peak = 0.0
    max_drawdown = 0.0
    for equity in equity_curve:
        peak = max(peak, equity)
        max_drawdown = max(max_drawdown, peak - equity)
    return max_drawdown


def _run_target_simple_momentum(
    *,
    ticks: list[TradeTick],
    config: NautilusHarnessConfig,
) -> NautilusHarnessMetrics:
    gross_pnl = 0.0
    fee_cost = 0.0
    turnover = 0.0
    order_count = 0
    holding_time_ns = 0
    equity = 0.0
    equity_curve: list[float] = [0.0]

    for current, nxt in zip(ticks, ticks[1:], strict=False):
        current_price = float(current.price)
        next_price = float(nxt.price)
        if current_price == next_price:
            continue

        quantity = config.fixed_notional_usd / current_price
        direction = 1.0 if next_price > current_price else -1.0
        trade_gross_pnl = (next_price - current_price) * quantity * direction
        trade_turnover = config.fixed_notional_usd * 2.0
        trade_fee_cost = trade_turnover * config.assumed_taker_fee_rate
        trade_net_pnl = trade_gross_pnl - trade_fee_cost

        gross_pnl += trade_gross_pnl
        fee_cost += trade_fee_cost
        turnover += trade_turnover
        order_count += 2
        holding_time_ns += max(0, nxt.ts_event - current.ts_event)
        equity += trade_net_pnl
        equity_curve.append(equity)

    if order_count > 0 and fee_cost <= 0.0:
        raise ValueError("fee_cost must be positive when target emits orders")
    funding_pnl = 0.0
    net_pnl = gross_pnl - fee_cost + funding_pnl
    if abs(net_pnl - equity_curve[-1]) > 1e-9:
        raise ValueError("net_pnl does not match closed-trade equity curve")

    ts_event_values = [tick.ts_event for tick in ticks]
    ts_init_values = [tick.ts_init for tick in ticks]
    elapsed_ns = max(0, max(ts_event_values) - min(ts_event_values))
    exposure_time_ratio = (
        min(1.0, holding_time_ns / elapsed_ns) if elapsed_ns > 0 else 0.0
    )
    round_trip_count = order_count // 2
    avg_holding_time = (
        (holding_time_ns / round_trip_count) / 1_000_000_000
        if round_trip_count
        else 0.0
    )

    return NautilusHarnessMetrics(
        gross_pnl=gross_pnl,
        fee_cost=fee_cost,
        funding_pnl=funding_pnl,
        net_pnl=net_pnl,
        trade_count=len(ticks),
        order_count=order_count,
        turnover=turnover,
        max_drawdown=compute_max_drawdown(tuple(equity_curve)),
        avg_holding_time=avg_holding_time,
        exposure_time_ratio=exposure_time_ratio,
        ts_event_min_ns=min(ts_event_values),
        ts_event_max_ns=max(ts_event_values),
        ts_init_min_ns=min(ts_init_values),
        ts_init_max_ns=max(ts_init_values),
        equity_curve=tuple(equity_curve),
    )


def _configure_high_level_path(
    *,
    root: Path,
    trading_date: date,
    symbol: str,
    ticks: list[TradeTick],
) -> NautilusHighLevelPathStatus:
    catalog_path = (
        root
        / "exports"
        / "truth"
        / "nautilus_high_level"
        / f"date={trading_date.isoformat()}"
        / f"symbol={symbol}"
    )
    catalog = ParquetDataCatalog(str(catalog_path))
    instrument = _hyperliquid_perpetual_instrument(
        symbol=symbol,
        ts_event=ticks[0].ts_event,
        ts_init=ticks[0].ts_init,
    )
    catalog.write_data([instrument])
    catalog.write_data(ticks, data_cls=TradeTick, identifier=str(ticks[0].instrument_id))
    data_config = BacktestDataConfig(
        catalog_path=str(catalog_path),
        data_cls="nautilus_trader.model.data:TradeTick",
        instrument_id=ticks[0].instrument_id,
    )
    venue_config = BacktestVenueConfig(
        name="HYPERLIQUID",
        oms_type="NETTING",
        account_type="MARGIN",
        starting_balances=["100000 USD"],
    )
    run_config = BacktestRunConfig(
        venues=[venue_config],
        data=[data_config],
        raise_exception=True,
    )
    node = BacktestNode(configs=[run_config])
    try:
        backtest_node_configured = True
    finally:
        node.dispose()

    return NautilusHighLevelPathStatus(
        status="configured",
        catalog_path=catalog_path.as_posix(),
        catalog_written=True,
        backtest_node_configured=backtest_node_configured,
        components=(
            "ParquetDataCatalog",
            "BacktestDataConfig",
            "BacktestVenueConfig",
            "BacktestRunConfig",
            "BacktestNode",
        ),
        note=(
            "P4-11 writes the target TradeTick universe into a ParquetDataCatalog "
            "with its instrument definition and validates BacktestNode configuration. "
            "Strategy metrics are computed by the v1 harness from that same target "
            "universe."
        ),
    )


def _hyperliquid_perpetual_instrument(
    *,
    symbol: str,
    ts_event: int,
    ts_init: int,
) -> CryptoPerpetual:
    if symbol != "BTC":
        raise ValueError("P4-11 Nautilus harness v1 is intentionally BTC-only")
    return CryptoPerpetual(
        instrument_id=InstrumentId(
            symbol=Symbol("BTC-USD-PERP"),
            venue=Venue("HYPERLIQUID"),
        ),
        raw_symbol=Symbol("BTC"),
        base_currency=BTC,
        quote_currency=USD,
        settlement_currency=USD,
        is_inverse=False,
        price_precision=8,
        price_increment=Price.from_str("0.00000001"),
        size_precision=8,
        size_increment=Quantity.from_str("0.00000001"),
        max_quantity=Quantity.from_str("1000.00000000"),
        min_quantity=Quantity.from_str("0.00000001"),
        max_notional=None,
        min_notional=Money(1.00, USD),
        max_price=Price.from_str("10000000.00000000"),
        min_price=Price.from_str("0.00000001"),
        margin_init=Decimal("0.0500"),
        margin_maint=Decimal("0.0250"),
        maker_fee=Decimal("0.000400"),
        taker_fee=Decimal("0.000400"),
        ts_event=ts_event,
        ts_init=ts_init,
    )
