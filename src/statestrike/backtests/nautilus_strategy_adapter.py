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
from statestrike.readiness import (
    BacktestReadinessReport,
    ReadinessProfileName,
    run_backtest_readiness,
)


class NautilusSingleTradeRoundtripConfig(StrategyConfig, frozen=True):
    instrument_id: InstrumentId
    trade_size: Decimal


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


class NautilusStrategyAdapterMetrics(BaseModel):
    model_config = ConfigDict(frozen=True)

    trade_count: int = Field(ge=0)
    order_count: int = Field(ge=0)
    position_count: int = Field(ge=0)
    gross_pnl: float
    fee_cost: float = Field(ge=0.0)
    net_pnl: float
    max_drawdown: float = Field(ge=0.0)
    backtest_start_ns: int | None = None
    backtest_end_ns: int | None = None
    run_id: str
    pnl_currency: str = "USD"
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

    control_result = run_nautilus_baseline_harness_v1(
        root=root,
        trading_date=trading_date,
        symbol=symbol,
        profile=profile,
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
    )
    instrument_id = ticks[0].instrument_id
    engine_config = BacktestEngineConfig(
        strategies=[
            ImportableStrategyConfig(
                strategy_path=(
                    "statestrike.backtests.nautilus_strategy_adapter:"
                    "NautilusSingleTradeRoundtripStrategy"
                ),
                config_path=(
                    "statestrike.backtests.nautilus_strategy_adapter:"
                    "NautilusSingleTradeRoundtripConfig"
                ),
                config={
                    "instrument_id": str(instrument_id),
                    "trade_size": str(trade_size),
                },
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
        gross_pnl = net_pnl + fee_cost
    finally:
        node.dispose()

    metrics = NautilusStrategyAdapterMetrics(
        trade_count=int(result.iterations),
        order_count=int(result.total_orders),
        position_count=int(result.total_positions),
        gross_pnl=gross_pnl,
        fee_cost=fee_cost,
        net_pnl=net_pnl,
        max_drawdown=compute_max_drawdown((0.0, net_pnl)),
        backtest_start_ns=result.backtest_start,
        backtest_end_ns=result.backtest_end,
        run_id=result.run_id,
    )
    if metrics.order_count <= 0:
        raise ValueError("Nautilus strategy adapter did not emit orders")
    if metrics.fee_cost <= 0.0:
        raise ValueError("Nautilus strategy adapter did not record positive fees")

    parity = NautilusStrategyAdapterParity(
        same_symbol=control_result.symbol == symbol,
        same_derived_input=control_result.metrics.trade_count == metrics.trade_count,
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


def _write_strategy_catalog(
    *,
    root: Path,
    trading_date: date,
    symbol: str,
    ticks: list[TradeTick],
) -> Path:
    catalog_path = (
        root
        / "exports"
        / "truth"
        / "nautilus_strategy_adapter"
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


def _fee_cost_from_orders(orders: tuple[Any, ...]) -> float:
    fee_cost = 0.0
    for order in orders:
        events = getattr(order, "events", ())
        for event in events:
            commission = getattr(event, "commission", None)
            if commission is not None:
                fee_cost += abs(float(commission))
    return fee_cost


def _net_pnl_from_result(result: Any) -> float:
    pnl_by_currency = result.stats_pnls.get("USD") or {}
    if "PnL (total)" not in pnl_by_currency:
        raise ValueError("Nautilus BacktestResult missing USD PnL (total)")
    return float(pnl_by_currency["PnL (total)"])
