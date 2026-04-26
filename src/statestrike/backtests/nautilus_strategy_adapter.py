from __future__ import annotations

from datetime import date
from decimal import Decimal
from pathlib import Path
from typing import Any
from typing import Literal

from nautilus_trader.backtest.node import BacktestNode
from nautilus_trader.config import (
    BacktestDataConfig,
    BacktestEngineConfig,
    BacktestRunConfig,
    BacktestVenueConfig,
    ImportableStrategyConfig,
    StrategyConfig,
)
from nautilus_trader.model.data import TradeTick
from nautilus_trader.model.enums import OrderSide, TimeInForce
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.orders import MarketOrder
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog
from nautilus_trader.trading.strategy import Strategy
from pydantic import BaseModel, ConfigDict, Field

from statestrike.backtests.nautilus import _load_truth_trade_ticks
from statestrike.backtests.nautilus_harness import (
    NautilusHarnessResult,
    _hyperliquid_perpetual_instrument,
    compute_max_drawdown,
    run_nautilus_baseline_harness_v1,
)
from statestrike.funding_pnl import (
    FundingAugmentedPnlSummary,
    FundingPnlLedger,
    FundingPositionInterval,
    build_augmented_pnl_summary,
    build_funding_pnl_ledger,
)
from statestrike.readiness import (
    BacktestReadinessReport,
    ReadinessProfileName,
    run_backtest_readiness,
)


class NautilusSingleTradeRoundtripConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    trade_size: Decimal


class NautilusSimpleMomentumConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    trade_size: Decimal
    max_roundtrips: int = 5


class NautilusSingleTradeRoundtripStrategy(Strategy):
    def __init__(self, config: NautilusSingleTradeRoundtripConfig) -> None:
        super().__init__(config)
        self._tick_count = 0
        self._entry_submitted = False
        self._exit_submitted = False
        self.instrument = None

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return
        self.subscribe_trade_ticks(self.config.instrument_id)

    def on_trade_tick(self, tick: TradeTick) -> None:
        self._tick_count += 1
        if not self._entry_submitted:
            self._submit_market(OrderSide.BUY)
            self._entry_submitted = True
            return
        if self._tick_count >= 2 and not self._exit_submitted:
            self._submit_market(OrderSide.SELL)
            self._exit_submitted = True

    def _submit_market(self, order_side: OrderSide) -> None:
        order: MarketOrder = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=order_side,
            quantity=Quantity(self.config.trade_size, 8),
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)


class NautilusSimpleMomentumStrategy(Strategy):
    def __init__(self, config: NautilusSimpleMomentumConfig) -> None:
        super().__init__(config)
        self.instrument = None
        self._previous_price: float | None = None
        self._position_open = False
        self._roundtrips_completed = 0

    def on_start(self) -> None:
        self.instrument = self.cache.instrument(self.config.instrument_id)
        if self.instrument is None:
            self.log.error(f"Could not find instrument for {self.config.instrument_id}")
            self.stop()
            return
        self.subscribe_trade_ticks(self.config.instrument_id)

    def on_trade_tick(self, tick: TradeTick) -> None:
        price = float(tick.price)
        if self._previous_price is None:
            self._previous_price = price
            return
        if self._roundtrips_completed >= self.config.max_roundtrips:
            self._previous_price = price
            return

        if not self._position_open and price > self._previous_price:
            self._submit_market(OrderSide.BUY)
            self._position_open = True
        elif self._position_open:
            self._submit_market(OrderSide.SELL)
            self._position_open = False
            self._roundtrips_completed += 1
        self._previous_price = price

    def _submit_market(self, order_side: OrderSide) -> None:
        order: MarketOrder = self.order_factory.market(
            instrument_id=self.config.instrument_id,
            order_side=order_side,
            quantity=Quantity(self.config.trade_size, 8),
            time_in_force=TimeInForce.GTC,
        )
        self.submit_order(order)


class NautilusStrategyAdapterMetrics(BaseModel):
    model_config = ConfigDict(frozen=True)

    input_trade_tick_count: int = Field(ge=0)
    executed_order_count: int = Field(ge=0)
    fill_event_count: int = Field(ge=0)
    closed_position_count: int = Field(ge=0)
    open_position_count_end: int = Field(ge=0)
    fee_cost: float = Field(ge=0.0)
    net_pnl: float
    gross_pnl: float
    funding_pnl: float = 0.0
    max_drawdown: float = Field(ge=0.0)
    equity_curve: tuple[float, ...]
    equity_curve_source: str = "PositionClosed.realized_pnl"
    position_intervals: tuple[FundingPositionInterval, ...] = ()
    position_interval_count: int = Field(default=0, ge=0)
    backtest_start_ns: int | None = None
    backtest_end_ns: int | None = None
    run_id: str
    pnl_currency: str = "USD"
    fee_cost_source: str = "sum(OrderFilled.commission)"
    net_pnl_source: str = 'BacktestResult.stats_pnls["USD"]["PnL (total)"]'
    gross_pnl_source: str = "derived: net_pnl + fee_cost"
    funding_treatment: Literal["ignored"] = "ignored"
    slippage_assumption: float = 0.0
    assumed_taker_fee_rate: float = 0.0004
    assumed_maker_fee_rate: float = 0.0004
    source: Literal["nautilus_backtest_result_and_engine_cache"] = (
        "nautilus_backtest_result_and_engine_cache"
    )


class NautilusStrategyAdapterParity(BaseModel):
    model_config = ConfigDict(frozen=True)

    same_symbol: bool
    same_derived_input: bool
    exact_pnl_match_required: bool = False
    exact_fill_count_match_required: bool = False
    explanation: str


class NautilusStrategyAdapterResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_name: str
    symbol: str
    date_range: tuple[str, str]
    dataset_profile: str
    readiness_status: str
    catalog_path: str
    control_result: NautilusHarnessResult
    nautilus_result: NautilusStrategyAdapterMetrics
    parity: NautilusStrategyAdapterParity
    readiness_report: BacktestReadinessReport


class NautilusFundingPnlIntegrationResult(BaseModel):
    model_config = ConfigDict(frozen=True)

    strategy_name: str
    symbol: str
    date_range: tuple[str, str]
    dataset_profile: str
    readiness_status: str
    baseline_result: NautilusStrategyAdapterResult
    funding_ledger: FundingPnlLedger
    augmented_pnl: FundingAugmentedPnlSummary
    funding_treatment: Literal["ex_post_ledger"] = "ex_post_ledger"
    note: str = (
        "Nautilus core accounting remains the source for net_pnl_ex_funding and "
        "fee_cost; funding_pnl is calculated after the run from official "
        "fundingHistory and signed position intervals."
    )


def run_nautilus_strategy_adapter(
    *,
    root: Path,
    trading_date: date,
    symbol: str,
    profile: ReadinessProfileName = "nautilus_baseline_candidate",
    trade_size: Decimal = Decimal("0.01"),
) -> NautilusStrategyAdapterResult:
    symbol = symbol.upper()
    readiness_report = run_backtest_readiness(
        root=root,
        trading_date=trading_date,
        symbols=(symbol,),
        profile=profile,
    )
    if readiness_report.status != "ready" or readiness_report.warning_reasons:
        reasons = readiness_report.blocking_reasons or readiness_report.warning_reasons
        raise ValueError(
            "Nautilus strategy adapter requires warning-free ready candidate: "
            + ", ".join(reasons)
        )

    ticks = _load_truth_trade_ticks(
        root=root,
        trading_date=trading_date,
        symbol=symbol,
    )
    if len(ticks) < 2:
        raise ValueError("Nautilus strategy adapter requires at least 2 trade ticks")

    catalog_path = _write_strategy_catalog(
        root=root,
        trading_date=trading_date,
        symbol=symbol,
        ticks=ticks,
        target="nautilus_strategy_adapter",
    )
    control_result = run_nautilus_baseline_harness_v1(
        root=root,
        trading_date=trading_date,
        symbol=symbol,
        profile=profile,
    )
    metrics = _run_nautilus_strategy(
        catalog_path=catalog_path,
        instrument_id=ticks[0].instrument_id,
        strategy_path=(
            "statestrike.backtests.nautilus_strategy_adapter:"
            "NautilusSingleTradeRoundtripStrategy"
        ),
        config_path=(
            "statestrike.backtests.nautilus_strategy_adapter:"
            "NautilusSingleTradeRoundtripConfig"
        ),
        config={
            "instrument_id": str(ticks[0].instrument_id),
            "trade_size": str(trade_size),
        },
    )
    if metrics.executed_order_count <= 0:
        raise ValueError("Nautilus strategy adapter did not emit orders")
    if metrics.fee_cost <= 0.0:
        raise ValueError("Nautilus strategy adapter did not record positive fees")

    parity = NautilusStrategyAdapterParity(
        same_symbol=control_result.symbol == symbol,
        same_derived_input=(
            control_result.metrics.trade_count == metrics.input_trade_tick_count
        ),
        explanation=(
            "P4-11.1 treats the Nautilus BacktestNode result as authoritative for "
            "orders and accounting. The P4-11A harness remains a control over the "
            "same derived input; exact PnL and fill-count parity are intentionally "
            "not required because Nautilus venue, execution, and portfolio accounting "
            "own the target result."
        ),
    )
    return NautilusStrategyAdapterResult(
        strategy_name="nautilus_single_trade_roundtrip_strategy",
        symbol=symbol,
        date_range=(trading_date.isoformat(), trading_date.isoformat()),
        dataset_profile=readiness_report.profile.name,
        readiness_status=readiness_report.status,
        catalog_path=catalog_path.as_posix(),
        control_result=control_result,
        nautilus_result=metrics,
        parity=parity,
        readiness_report=readiness_report,
    )


def run_nautilus_repeated_order_strategy_harness(
    *,
    root: Path,
    trading_date: date,
    symbol: str,
    profile: ReadinessProfileName = "nautilus_baseline_candidate",
    trade_size: Decimal = Decimal("0.01"),
    max_roundtrips: int = 5,
) -> NautilusStrategyAdapterResult:
    symbol = symbol.upper()
    readiness_report = run_backtest_readiness(
        root=root,
        trading_date=trading_date,
        symbols=(symbol,),
        profile=profile,
    )
    if readiness_report.status != "ready" or readiness_report.warning_reasons:
        reasons = readiness_report.blocking_reasons or readiness_report.warning_reasons
        raise ValueError(
            "Nautilus repeated-order strategy requires warning-free ready candidate: "
            + ", ".join(reasons)
        )
    ticks = _load_truth_trade_ticks(
        root=root,
        trading_date=trading_date,
        symbol=symbol,
    )
    if len(ticks) < 5:
        raise ValueError(
            "Nautilus repeated-order strategy requires at least 5 trade ticks"
        )
    catalog_path = _write_strategy_catalog(
        root=root,
        trading_date=trading_date,
        symbol=symbol,
        ticks=ticks,
        target="nautilus_repeated_order_strategy",
    )
    control_result = run_nautilus_baseline_harness_v1(
        root=root,
        trading_date=trading_date,
        symbol=symbol,
        profile=profile,
    )
    metrics = _run_nautilus_strategy(
        catalog_path=catalog_path,
        instrument_id=ticks[0].instrument_id,
        strategy_path=(
            "statestrike.backtests.nautilus_strategy_adapter:"
            "NautilusSimpleMomentumStrategy"
        ),
        config_path=(
            "statestrike.backtests.nautilus_strategy_adapter:"
            "NautilusSimpleMomentumConfig"
        ),
        config={
            "instrument_id": str(ticks[0].instrument_id),
            "trade_size": str(trade_size),
            "max_roundtrips": max_roundtrips,
        },
    )
    if metrics.executed_order_count <= 2:
        raise ValueError("Nautilus repeated-order strategy did not emit multiple orders")
    if metrics.fill_event_count <= 2:
        raise ValueError(
            "Nautilus repeated-order strategy did not produce multiple fills"
        )
    if metrics.closed_position_count < 1:
        raise ValueError("Nautilus repeated-order strategy did not close a position")
    if metrics.fee_cost <= 0.0:
        raise ValueError("Nautilus repeated-order strategy did not record positive fees")

    parity = NautilusStrategyAdapterParity(
        same_symbol=control_result.symbol == symbol,
        same_derived_input=(
            control_result.metrics.trade_count == metrics.input_trade_tick_count
        ),
        explanation=(
            "P4-11.2 treats the Nautilus BacktestNode repeated-order result as "
            "authoritative for orders, fills, positions, fees, and accounting. "
            "The P4-11A harness remains a control over the same derived input; "
            "exact PnL and fill-count parity are intentionally not required."
        ),
    )
    return NautilusStrategyAdapterResult(
        strategy_name="nautilus_simple_momentum_strategy",
        symbol=symbol,
        date_range=(trading_date.isoformat(), trading_date.isoformat()),
        dataset_profile=readiness_report.profile.name,
        readiness_status=readiness_report.status,
        catalog_path=catalog_path.as_posix(),
        control_result=control_result,
        nautilus_result=metrics,
        parity=parity,
        readiness_report=readiness_report,
    )


def run_nautilus_repeated_order_strategy_with_funding_pnl(
    *,
    root: Path,
    trading_date: date,
    symbol: str,
    profile: ReadinessProfileName = "nautilus_funding_candidate",
    trade_size: Decimal = Decimal("0.01"),
    max_roundtrips: int = 5,
) -> NautilusFundingPnlIntegrationResult:
    baseline = run_nautilus_repeated_order_strategy_harness(
        root=root,
        trading_date=trading_date,
        symbol=symbol,
        profile=profile,
        trade_size=trade_size,
        max_roundtrips=max_roundtrips,
    )
    funding_ledger = build_funding_pnl_ledger(
        root=root,
        trading_date=trading_date,
        symbol=symbol,
        position_intervals=baseline.nautilus_result.position_intervals,
    )
    augmented_pnl = build_augmented_pnl_summary(
        gross_pnl_ex_funding=baseline.nautilus_result.gross_pnl,
        fee_cost=baseline.nautilus_result.fee_cost,
        net_pnl_ex_funding=baseline.nautilus_result.net_pnl,
        funding_pnl=funding_ledger.funding_pnl,
    )
    return NautilusFundingPnlIntegrationResult(
        strategy_name="nautilus_simple_momentum_strategy_with_funding_pnl",
        symbol=baseline.symbol,
        date_range=baseline.date_range,
        dataset_profile=baseline.dataset_profile,
        readiness_status=baseline.readiness_status,
        baseline_result=baseline,
        funding_ledger=funding_ledger,
        augmented_pnl=augmented_pnl,
    )


def _write_strategy_catalog(
    *,
    root: Path,
    trading_date: date,
    symbol: str,
    ticks: list[TradeTick],
    target: str,
) -> Path:
    catalog_path = (
        root
        / "exports"
        / "truth"
        / target
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
    return catalog_path


def _run_nautilus_strategy(
    *,
    catalog_path: Path,
    instrument_id: InstrumentId,
    strategy_path: str,
    config_path: str,
    config: dict[str, object],
) -> NautilusStrategyAdapterMetrics:
    engine_config = BacktestEngineConfig(
        strategies=[
            ImportableStrategyConfig(
                strategy_path=strategy_path,
                config_path=config_path,
                config=config,
            )
        ],
        run_analysis=True,
    )
    data_config = BacktestDataConfig(
        catalog_path=str(catalog_path),
        data_cls="nautilus_trader.model.data:TradeTick",
        instrument_id=instrument_id,
    )
    venue_config = BacktestVenueConfig(
        name="HYPERLIQUID",
        oms_type="NETTING",
        account_type="MARGIN",
        starting_balances=["100000 USD"],
        trade_execution=True,
    )
    run_config = BacktestRunConfig(
        venues=[venue_config],
        data=[data_config],
        engine=engine_config,
        raise_exception=True,
        dispose_on_completion=False,
    )
    node = BacktestNode(configs=[run_config])
    try:
        result = node.run()[0]
        engine = node.get_engines()[0]
        orders = tuple(engine.cache.orders())
        positions = tuple(engine.cache.positions())
        fee_cost = _fee_cost_from_orders(orders)
        net_pnl = _net_pnl_from_result(result)
        fill_event_count = _fill_event_count_from_orders(orders)
        position_intervals = _position_intervals_from_orders(
            orders=orders,
            symbol=str(instrument_id.symbol).split("-")[0],
        )
        closed_position_count = int(engine.cache.positions_closed_count())
        open_position_count_end = int(engine.cache.positions_open_count())
        equity_curve = _equity_curve_from_positions(positions)
    finally:
        node.dispose()

    if len(equity_curve) <= 1:
        equity_curve = (0.0, net_pnl)
    if open_position_count_end == 0 and abs(equity_curve[-1] - net_pnl) > 1e-9:
        raise ValueError("Nautilus net_pnl does not match closed-position equity curve")

    return NautilusStrategyAdapterMetrics(
        input_trade_tick_count=int(result.iterations),
        executed_order_count=int(result.total_orders),
        fill_event_count=fill_event_count,
        closed_position_count=closed_position_count,
        open_position_count_end=open_position_count_end,
        fee_cost=fee_cost,
        net_pnl=net_pnl,
        gross_pnl=net_pnl + fee_cost,
        max_drawdown=compute_max_drawdown(equity_curve),
        equity_curve=equity_curve,
        backtest_start_ns=result.backtest_start,
        backtest_end_ns=result.backtest_end,
        run_id=result.run_id,
        position_intervals=position_intervals,
        position_interval_count=len(position_intervals),
    )


def _fee_cost_from_orders(orders: tuple[Any, ...]) -> float:
    fee_cost = 0.0
    for order in orders:
        events = getattr(order, "events", ())
        for event in events:
            commission = getattr(event, "commission", None)
            if commission is not None:
                fee_cost += abs(float(commission))
    return fee_cost


def _fill_event_count_from_orders(orders: tuple[Any, ...]) -> int:
    count = 0
    for order in orders:
        events = getattr(order, "events", ())
        for event in events:
            if event.__class__.__name__ == "OrderFilled":
                count += 1
    return count


def _position_intervals_from_orders(
    *,
    orders: tuple[Any, ...],
    symbol: str,
) -> tuple[FundingPositionInterval, ...]:
    zero_tolerance = 1e-12
    fills: list[tuple[int, int, float]] = []
    sequence = 0
    for order in orders:
        events = getattr(order, "events", ())
        for event in events:
            if event.__class__.__name__ != "OrderFilled":
                continue
            order_side_value = getattr(event, "order_side", "")
            order_side = str(
                getattr(order_side_value, "name", order_side_value)
            ).upper()
            quantity = float(getattr(event, "last_qty"))
            signed_delta = quantity if "BUY" in order_side else -quantity
            fills.append((int(getattr(event, "ts_event")), sequence, signed_delta))
            sequence += 1

    position = 0.0
    opened_ts_ns: int | None = None
    intervals: list[FundingPositionInterval] = []
    for ts_event, _, signed_delta in sorted(fills):
        if opened_ts_ns is not None and abs(position) > zero_tolerance:
            intervals.append(
                FundingPositionInterval(
                    symbol=symbol,
                    opened_ts_ns=opened_ts_ns,
                    closed_ts_ns=ts_event,
                    signed_position_size=position,
                    source="nautilus_order_fills",
                )
            )
        position += signed_delta
        if abs(position) <= zero_tolerance:
            position = 0.0
        opened_ts_ns = ts_event if position != 0.0 else None

    if opened_ts_ns is not None and abs(position) > zero_tolerance:
        intervals.append(
            FundingPositionInterval(
                symbol=symbol,
                opened_ts_ns=opened_ts_ns,
                closed_ts_ns=None,
                signed_position_size=position,
                source="nautilus_order_fills",
            )
        )
    return tuple(intervals)


def _equity_curve_from_positions(positions: tuple[Any, ...]) -> tuple[float, ...]:
    closed_pnls: list[tuple[int, float]] = []
    for position in positions:
        events = getattr(position, "events", ())
        for event in events:
            if event.__class__.__name__ != "PositionClosed":
                continue
            realized_pnl = getattr(event, "realized_pnl", None)
            if realized_pnl is None:
                continue
            ts_event = int(getattr(event, "ts_event", 0))
            closed_pnls.append((ts_event, float(realized_pnl)))

    equity = 0.0
    equity_curve = [0.0]
    for _, realized_pnl in sorted(closed_pnls):
        equity += realized_pnl
        equity_curve.append(equity)
    return tuple(equity_curve)


def _net_pnl_from_result(result: Any) -> float:
    pnl_by_currency = result.stats_pnls.get("USD") or {}
    if "PnL (total)" not in pnl_by_currency:
        raise ValueError("Nautilus BacktestResult missing USD PnL (total)")
    return float(pnl_by_currency["PnL (total)"])
