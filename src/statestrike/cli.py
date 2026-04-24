from __future__ import annotations

import argparse
import json
from datetime import date
from pathlib import Path

from statestrike import smoke
from statestrike.exports import export_hftbacktest_npz, export_nautilus_catalog
from statestrike.quality import run_quality_audit
from statestrike.settings import Settings


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "collect-smoke":
        return smoke.main(_smoke_argv_from_cli(args, campaign_mode=False))
    if args.command == "run-phase1-smoke":
        return smoke.main(_smoke_argv_from_cli(args, campaign_mode=True))
    if args.command == "audit-quality":
        settings = _settings_from_cli(args)
        report = run_quality_audit(
            normalized_root=settings.data_root,
            quarantine_root=settings.data_root,
            trading_date=date.fromisoformat(args.date),
            symbols=_parse_symbols(args.symbols),
            skew_warning_ms=settings.smoke_skew_warning_ms,
            skew_severe_ms=settings.smoke_skew_severe_ms,
            asset_ctx_stale_threshold_ms=settings.smoke_asset_ctx_stale_threshold_ms,
            trade_gap_threshold_ms=settings.smoke_trade_gap_threshold_ms,
            quarantine_warning_rate=settings.smoke_quarantine_warning_rate,
            quarantine_severe_rate=settings.smoke_quarantine_severe_rate,
        )
        print(json.dumps(report.model_dump(mode="json"), indent=2, ensure_ascii=True))
        return 0
    if args.command == "export-nautilus":
        settings = _settings_from_cli(args)
        path = export_nautilus_catalog(
            normalized_root=settings.data_root,
            export_root=settings.data_root,
            trading_date=date.fromisoformat(args.date),
            symbol=args.symbol,
        )
        print(json.dumps({"path": str(path)}, ensure_ascii=True))
        return 0
    if args.command == "export-hft":
        settings = _settings_from_cli(args)
        path = export_hftbacktest_npz(
            normalized_root=settings.data_root,
            export_root=settings.data_root,
            trading_date=date.fromisoformat(args.date),
            symbol=args.symbol,
        )
        print(json.dumps({"path": str(path)}, ensure_ascii=True))
        return 0
    raise ValueError(f"unsupported command: {args.command}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="statestrike")
    subparsers = parser.add_subparsers(dest="command", required=True)

    for command in ("collect-smoke", "run-phase1-smoke"):
        subparser = subparsers.add_parser(command)
        subparser.add_argument("--data-root")
        subparser.add_argument("--network", choices=("mainnet", "testnet"), default="mainnet")
        subparser.add_argument("--symbols", required=True)
        subparser.add_argument(
            "--channels",
            default="l2Book,trades,activeAssetCtx,candle",
        )
        subparser.add_argument("--candle-interval", default="1m")
        subparser.add_argument("--duration-hours", type=int, required=True)
        subparser.add_argument("--capture-session-id")
        subparser.add_argument("--date")
        subparser.add_argument("--max-messages", type=int)
        subparser.add_argument("--ping-interval-seconds", type=int)
        subparser.add_argument("--reconnect-limit", type=int)

    audit = subparsers.add_parser("audit-quality")
    audit.add_argument("--data-root")
    audit.add_argument("--date", required=True)
    audit.add_argument("--symbols", required=True)

    export_nautilus = subparsers.add_parser("export-nautilus")
    export_nautilus.add_argument("--data-root")
    export_nautilus.add_argument("--date", required=True)
    export_nautilus.add_argument("--symbol", required=True)

    export_hft = subparsers.add_parser("export-hft")
    export_hft.add_argument("--data-root")
    export_hft.add_argument("--date", required=True)
    export_hft.add_argument("--symbol", required=True)

    return parser


def _smoke_argv_from_cli(args: argparse.Namespace, *, campaign_mode: bool) -> list[str]:
    smoke_argv = [
        "--market-data-network",
        args.network,
        "--allowed-symbols",
        args.symbols,
        "--channels",
        args.channels,
        "--candle-interval",
        args.candle_interval,
        "--max-runtime-seconds",
        str(args.duration_hours * 3600),
    ]
    if args.data_root is not None:
        smoke_argv.extend(["--data-root", args.data_root])
    if args.capture_session_id is not None:
        smoke_argv.extend(["--capture-session-id", args.capture_session_id])
    if args.date is not None:
        smoke_argv.extend(["--date", args.date])
    if args.max_messages is not None:
        smoke_argv.extend(["--max-messages", str(args.max_messages)])
    if args.ping_interval_seconds is not None:
        smoke_argv.extend(["--ping-interval-seconds", str(args.ping_interval_seconds)])
    if args.reconnect_limit is not None:
        smoke_argv.extend(["--reconnect-limit", str(args.reconnect_limit)])
    if campaign_mode:
        smoke_argv.append("--campaign-mode")
    return smoke_argv


def _settings_from_cli(args: argparse.Namespace) -> Settings:
    overrides: dict[str, object] = {}
    if getattr(args, "data_root", None) is not None:
        overrides["data_root"] = Path(args.data_root)
    return Settings(**overrides)


def _parse_symbols(symbols: str) -> tuple[str, ...]:
    return tuple(symbol.strip().upper() for symbol in symbols.split(",") if symbol.strip())


if __name__ == "__main__":
    raise SystemExit(main())
