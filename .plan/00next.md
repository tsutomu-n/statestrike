 # StateStrike Phase 1 データ基盤実装計画 v1

  ## 要約

  - 目的は、現在の bootstrap 実装を出発点にして、Phase 1 の data platform を完成させることです。
  - ここで完成させるのは、l2_book / trades / active_asset_ctx の market-data collector、raw 保存、normalized 変換、Pandera schema gate、DuckDB
    quality audit、Nautilus export、hftbacktest export までです。
  - やらないことは、live 注文経路、署名、user stream、paper/tiny-live/live、add-on、bracket attach、本番 alerting の完成です。
  - 現在すでにある土台は維持します。具体的には pyproject.toml、settings.py、identifiers.py、models.py、policies.py、paths.py と既存テストは壊さ
    ず、その上に Phase 1 本体を足します。

  ## 目的

  - Hyperliquid mainnet の実市場データを truth source として、再現可能な raw/normalized/export パイプラインを作る。
  - 将来の Nautilus execution-aware backtest と hftbacktest calibration にそのまま渡せる compare-ready artifacts を作る。
  - strategy 実装より先に、欠損・重複・timestamp skew・book continuity・schema drift を検知できる基盤を固める。

  ## 制約

  - Phase 1 は market channels only に固定する。対象 channel は l2_book、trades、active_asset_ctx のみ。
  - source of truth は混ぜない。Pybotters = collector truth、Hyperliquid official Info/S3 = 補助、Nautilus = execution export target、
    hftbacktest = calibration export target。
  - パッケージ構成はこの phase では flat module 維持 とし、src/statestrike/ 配下に追加する。大きな subpackage 分割はまだやらない。
  - raw は jsonl.zst、normalized と quarantine と export 中間成果物は parquet + zstd を使う。
  - l2_book の normalized 表現は book_events + book_levels の 2 テーブル固定。
  - capture_session_id = UUIDv7、reconnect_epoch と book_epoch は別管理、gap policy は既存 policies.py の方針を拡張する。
  - symbol allowlist は コードにハードコードしない。runtime config で与える。デフォルトは空でよい。
  - 依存は uv.lock の exact pin を維持し、collector/schema/export に関わるライブラリは range 指定にしない。

  ## 対象ファイル

  - 既存を更新するファイル: pyproject.toml, src/statestrike/settings.py, src/statestrike/models.py, src/statestrike/policies.py, src/
    statestrike/paths.py
  - 新規追加する実装ファイル: src/statestrike/collector.py, src/statestrike/storage.py, src/statestrike/normalize.py, src/statestrike/
    schemas.py, src/statestrike/quality.py, src/statestrike/exports.py
  - 新規追加するテストと fixture: tests/test_collector.py, tests/test_storage.py, tests/test_normalize.py, tests/test_schemas.py, tests/
    test_quality.py, tests/test_exports.py, tests/fixtures/hyperliquid/

  ## 公開 API / 追加型

  - collector.py
      - CollectorConfig: allowed_symbols, source_priority, market_data_network, flush_interval_ms, snapshot_recovery_enabled を持つ。
      - CollectorBatchResult: 1 回の収集バッチの件数、disconnect/reconnect 回数、gap flag を返す。
      - collect_market_batch(...): Pybotters で market channels を購読し、raw writer と normalizer に流す。
  - storage.py
      - RawWriter: raw_ws/date=.../channel=.../symbol=.../session=.../*.jsonl.zst へ append-safe に書く。
      - NormalizedWriter: normalized parquet を normalized/<table>/date=.../symbol=.../ に書く。
      - QuarantineWriter: schema fail 行を quarantine/<table>/date=.../symbol=.../ に隔離する。
      - ManifestRecord: capture_session_id, started_at, ended_at, channels, symbols, row_count, ws_disconnect_count, reconnect_count, gap_flags
        を持つ。
  - normalize.py
      - normalize_l2_book(...): raw payload から book_events と book_levels を作る。
      - normalize_trades(...): raw payload から TradeEvent 行を作る。
      - normalize_asset_ctx(...): raw payload から AssetContextEvent 行を作る。
      - すべての normalizer は dedup hash を付け、capture_session_id、reconnect_epoch、book_epoch、source を埋める。
  - schemas.py
      - BOOK_EVENTS_SCHEMA, BOOK_LEVELS_SCHEMA, TRADES_SCHEMA, ASSET_CTX_SCHEMA
      - Pandera は strict=True, coerce=True を基本にし、unexpected columns は reject する。
  - quality.py
      - QualityAuditReport: channel 別 row count、gap count、skew summary、crossed book count、zero/negative qty count、asset_ctx stale count
        を持つ。
      - run_quality_audit(...): DuckDB で normalized parquet を読んで JSON/Markdown 向け summary を返す。
  - exports.py
      - export_nautilus_catalog(...): Nautilus Parquet catalog 形式へ書き出す。対象は instrument metadata、trade ticks、order-book data、asset
        context 補助データ。
      - export_hftbacktest_npz(...): hftbacktest 公式 format に合わせた 6 列の .npz を出力する。列は event, exch_timestamp, local_timestamp,
        side, price, qty。
      - Nautilus instrument id は BTC-USD-PERP.HYPERLIQUID 形式に固定する。

  ## 実装方針

  - settings.py に collector/storage/export 用の phase1 設定を追加する。ただし live 注文系 flag は引き続き reject する。
  - paths.py は raw/quarantine/export path builder を追加し、既存 normalized/export 規約と揃える。
  - models.py は raw payload 用の一時 model は作らず、normalized canonical model に集中する。必要なら ManifestRecord と export metadata model
    を追加する。
  - collector.py は network client と business logic を分離し、Pybotters DataStore 読み出しと writer 呼び出しの境界を明確にする。
  - storage.py は書き込み責務だけを持ち、schema 判定や audit 判定は行わない。
  - normalize.py で canonical hash を一元生成し、book_epoch 更新は policies.py の判定結果に従う。
  - schemas.py は DataFrame 単位の Pandera schema を持ち、validation fail は exception を投げるのではなく quarantine 用の結果オブジェクトに変換
    できるようにする。
  - quality.py は HTML を作らない。phase 1 は DuckDB query + JSON/Markdown summary だけでよい。
  - exports.py は raw から直接書き出さず、normalized parquet だけを入力に使う。
  - CI では network を使わない。collector の挙動は fixture replay で担保する。

  ## テスト方針

  - 固定のテストコマンドは CI=true timeout 120 uv run pytest -q。
  - unit test
      - settings: phase1 制約、mainnet default、source priority、allowlist parsing
      - identifiers: deterministic hash、intent id、cloid、UUIDv7
      - models/policies/paths: 既存 coverage を維持
  - fixture replay test
      - tests/fixtures/hyperliquid/ に l2_book, trades, active_asset_ctx の sample payload を置く
      - raw payload -> normalize -> Pandera validate -> parquet write までを一気通しで確認する
  - schema test
      - 必須列欠落、型 coercion、unexpected columns、px > 0、qty > 0、timestamp 逆転を検証する
      - invalid rows が quarantine に出ることを検証する
  - quality test
      - l2_book continuity 不明時に non_recoverable になること
      - crossed book、negative size、stale asset_ctx を集計できること
  - export test
  - regression test

  ## 完了条件

  - l2_book / trades / active_asset_ctx の fixture replay から raw、normalized、quarantine、audit、export まで通る。
  - raw 保存で jsonl.zst が生成され、manifest に capture_session_id、reconnect 回数、gap flag が残る。
  - normalized parquet が book_events, book_levels, trades, asset_ctx の 4 系統で生成される。
  - Pandera schema gate が invalid rows を quarantine へ分離し、正常系と混在しない。
  - DuckDB quality audit が gap/skew/book sanity/asset_ctx staleness の summary を返す。
  - Nautilus catalog export と hftbacktest .npz export が fixture ベースで再現可能に生成される。
  - CI=true timeout 120 uv run pytest -q が green。
  - live 注文経路、user streams、paper/tiny-live/live のコードはこの phase に入っていない。

  ## 前提として固定する仮定

  - 今回の計画は bootstrap のやり直しではなく、bootstrap の上に Phase 1 本体を積む前提です。
  - True profile、signal 実装、paper/live、baseline strategy engine の拡張はこの計画に含めません。
  - compare report そのものはまだ作らず、compare-ready artifacts までを phase 1 完了とみなします。