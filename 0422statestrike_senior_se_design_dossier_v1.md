# StateStrike 第三者シニアSE向け 設計資料パック v1

- 作成日: 2026-04-22 JST
- ステータス: 採用版
- 対象読者: 第三者のシニアSE、アーキテクト、実装責任者、検証責任者、運用責任者
- 文書目的: StateStrike を第三者が引き継いで設計・実装・検証できるように、完成品前提の決定事項、責務分離、設計原則、運用前提、受入条件を一冊にまとめる
- 位置づけ: 概要資料ではなく、設計のための基準文書。実装者の裁量で勝手に変更してよい事項と、変更管理が必要な事項を切り分けるための資料

---

## 0. この資料の使い方

この資料は、これまで作成してきた完成品仕様・アーキテクチャ仕様・データ取得仕様のうち、**第三者のシニアSEが実際に設計に着手するために必要な部分を、決定済み事項を中心に再整理したパック**である。

この資料の最重要ポイントは次の 5 つである。

1. **実装前に迷ってはいけないことを先に固定する**。
   たとえば、collector の truth をどこに置くか、execution の truth をどこに置くか、live 本線に Fast を入れるか、isolated を崩すか、などは、実装中に揺らすと手戻りが大きい。
2. **collector / execution / calibration / validation の責務分離を守る**。
   StateStrike では「全部を 1 つのフレームワークでやる」思想を採らない。用途ごとに最適な OSS を割り当てる。
3. **設計は “signal を増やす” よりも “truth を分ける” を優先する**。
   再現性が低い戦略は、どれだけきれいなロジックでも live で壊れる。先に data truth と execution truth を固める。
4. **Fast は本番の利益の柱ではなく、研究・paper 用の lane として扱う**。
   低資金で回数を増やしたい動機は理解できるが、Fast を live 本線へ入れると、fee・spread・missed fill に edge を持っていかれやすい。Fast は開発速度と知見獲得には有益だが、完成品既定値にはしない。
5. **この資料は単独で読めるように作るが、実装時は附属仕様と照合してよい**。
   ただし矛盾がある場合は、**本資料の決定済み事項が最優先**である。

### 0.1 規範の優先順位

もし複数文書の間で矛盾が見えた場合、優先順位は次のとおりとする。

1. 本資料の「1. 最終決定事項」
2. 本資料の「2. 私たちの最終推奨」
3. 本資料に収録した「アーキテクチャ追補」
4. 末尾に埋め込んだ「完成品仕様書（基準版）」
5. 過去の設計メモ・中間仕様書・README 類

### 0.2 読み進め方

最短で全体を掴むには、次の順番で読めばよい。

1. **1章**: 最終決定事項
2. **2章**: 私たちの最終推奨
3. **3章**: システム全体像
4. **4章**: 第三者SEが最初に固めるべきこと
5. **5章**: 実装・検証・運用の境界
6. **付録A**: 完成品仕様書（詳細）

---

## 1. 最終決定事項

以下は、現時点で**決め切ってよい**事項である。第三者シニアSEは、これらを前提条件として設計・分解・実装順序を組めばよい。

### 1.1 取引所・商品・運用範囲

- 本番サポート取引所は **Hyperliquid のみ**
- 商品は **Perpetuals のみ**
- 運用は **isolated margin only**
- cross margin は本番既定値では使わない
- GRVT / Bitget / MEXC / CEX 現物 / オプションは完成品スコープ外
- Spot は oracle/value/basis 参照用に間接利用することはあっても、売買対象にはしない

### 1.2 戦略のスコープ

- live 本線は **Core + Re-entry**
- **Fast は research / paper / backtest 専用**
- Post-Liquidation Recovery は research 戦略であり、完成品既定値では無効
- ML は主戦略にしない。入れるとしても veto / size 補助に留める
- funding carry / arbitrage / MM / HFT は本線から除外する

### 1.3 ツールの責務分離

- **Pybotters = data truth**
- **Hyperliquid official WS/Info/S3 = venue truth / 補助 backfill / gap repair**
- **NautilusTrader = execution truth**
- **hftbacktest = queue / latency calibration truth**
- **DuckDB + Parquet = 分析 spine**
- **Pandera = schema / quality gate**
- **Tardis = optional long-history bootstrap**

### 1.4 データ取得の主方針

- **WS 主 / REST・S3 従**
- 主 collector は Pybotters
- 主収集対象は `l2_book`, `trades`, `active_asset_ctx`
- 補助対象は `candle`, `bbo`, `all_mids`
- live 監査用に `order_updates`, `user_fills`, `user_events`, `user_fundings`
- 保存は **raw と normalized の二層**
- normalized 入口に Pandera gate を置く

### 1.5 バックテストの主方針

- strategy correctness の一次評価は current engine でよい
- **execution-aware 損益の truth は NautilusTrader**
- maker/queue/latency の悲観・標準・楽観レンジは hftbacktest で出す
- hftbacktest は replay 型なので主バックテスターにはしない
- 最終採用は **current / Nautilus / paper / tiny-live / live** の差を見て決める

### 1.6 validation と採用ゲート

- walk-forward を必須にする
- paper を必須にする
- tiny-live を必須にする
- **DSR（Deflated Sharpe Ratio）を採用ゲートに入れる**
- trial registry を残し、試した特徴量・閾値・ family を記録する

### 1.7 シグナル構造

- 時間軸は **15m / 5m / 30s / 60s**
- 10s は execution 補助であり、主信号には使わない
- エントリーは **L1 Opportunity / L2 Tradable / L3 Executable** の 3 層
- lane は **Fast / Core / Re-entry** の 3 レーン
- ただし live 本線は **Core / Re-entry の 2 レーン**
- 回数を増やす主役は Fast ではなく **Re-entry**

### 1.8 リスク方針

- isolated only
- no top-up を既定値とする
- fixed fractional を主軸にする
- ATR stop を基礎にする
- daily stop / sleeve DD stop / strategy stop / stale feed stop を持つ
- Fast を live に入れない理由の一つは、リスクの質が悪化しやすいことにある

### 1.9 変更管理

以下は実装中に変えてはならない。

- truth の責務分離
- Hyperliquid only
- isolated only
- Fast を live 本線に入れないこと
- WS 主 / REST・S3 従
- normalized gate を前段に置くこと
- Nautilus = execution truth
- DSR gate を入れること

---

## 2. 私たちの最終推奨

ここでは、選択肢があるように見えても、**実務上どれを採るべきか**をはっきり書く。

### 2.1 強く推奨すること

1. **Pybotters を collector の主軸にする**
   理由: HyperliquidDataStore があり、既存運用知見もあり、collector のフルスクラッチを避けられる。

2. **NautilusTrader を execution truth にする**
   理由: Hyperliquid 統合、full-depth L2、reconciliation、event-driven な約定再現があり、current engine より live に近い。

3. **hftbacktest を queue/latency 校正専用にする**
   理由: maker fill / latency の悲観・標準・楽観レンジはここでしか取りにくいが、主バックテスターには向かない。

4. **DuckDB + Pandera を正式採用する**
   理由: quality audit と schema gate を collector の外に明示的に持てる。collector 成功と downstream 可否を分離できる。

5. **Fast は live 本線から外す**
   理由: signal 数は増やせても、低資金では fee / spread / missed fill の比率が高くなりやすい。Fast は研究と paper のために使う。

6. **Tardis は必要時のみ追加する**
   理由: collector 開始前の履歴が要るなら有効だが、必須ではない。依存とコストを増やしすぎない。

### 2.2 非推奨なこと

1. Pybotters 単独で collector から高精度バックテストまで完結させること
2. 公式 REST / S3 を主ソースにすること
3. hftbacktest を主バックテスターにすること
4. Fast を live 本線に入れること
5. OI を主シグナルにすること
6. funding 極端値だけで逆張ること
7. cross margin を使うこと

### 2.3 第三者SEへの実務的アドバイス

第三者のシニアSEがこの案件を引き継ぐとき、一番やってはいけないのは「collector を書き直す」「とりあえず全データを 1 本のバックテスターへ突っ込む」「Fast を live に入れて件数を稼ぐ」の 3 つである。

- collector は Pybotters を核にしてよい
- ただし collector そのものは品質監査層を持つ別システムとして設計する
- 現在の戦略の勝敗を分けるのは signal の派手さではなく execution fidelity である
- したがって、まず measurement / quality / Nautilus export を固めるべきであり、lane の微調整はその後でよい

---

## 3. システム全体像

StateStrike の完成品は、6 層で理解すると設計しやすい。

1. **Collector Layer**
2. **Normalization & Quality Layer**
3. **Research / Derived Layer**
4. **Execution-Aware Backtest Layer**
5. **Paper / Tiny-live / Live Layer**
6. **Validation / Audit / Reporting Layer**

### 3.1 Collector Layer

ここは Pybotters を中心に組む。
主対象は market streams と user streams である。

- market: `l2_book`, `trades`, `active_asset_ctx`, `candle`, `bbo`, `all_mids`
- user: `order_updates`, `user_fills`, `user_events`, `user_fundings`

### 3.2 Normalization & Quality Layer

ここが collector と downstream の境界である。
raw をそのまま流さず、normalized に変換し、Pandera gate を通してから保存する。

ここで検出するもの:
- 欠損
- 重複
- timestamp 逆行
- size / price の異常
- symbol 解決失敗
- sequence の飛び

### 3.3 Research / Derived Layer

ここでは feature を作る。
- 15m state
- 5m lane
- 30s flow
- 60s persistence
- cost surface
- edge table
- quality audit

DuckDB + Parquet を spine にする。

### 3.4 Execution-Aware Backtest Layer

ここは NautilusTrader を主軸にする。
current engine は引き続き持つが、execution-aware な損益 truth は Nautilus とする。

### 3.5 Paper / Tiny-live / Live Layer

ここは Hyperliquid の actual venue semantics を前提に動かす。 
SDK fallback はあってよいが、truth は Hyperliquid venue state。

### 3.6 Validation / Audit / Reporting Layer

- realized cost audit
- current vs Nautilus vs paper vs tiny-live vs live compare
- DSR
- trial registry
- incident journal
- daily report

---

## 4. 第三者シニアSEが最初に固めるべきもの

### 4.1 ディレクトリ責務

最低限、次の責務に分ける。

```txt
src/perpscope/
  collector/
  adapters/hyperliquid/
  normalize/
  quality/
  storage/
  features/
  state/
  signals/
  execution/
  live/
  paper/
  backtest/
  nautilus_bridge/
  calibration/
  validation/
  reports/
```

### 4.2 スキーマの優先順位

最初に固定すべきは、feature ではなく schema である。
最低限の canonical columns は次。

- `exchange_ts`
- `recv_ts`
- `symbol`
- `capture_session_id`
- `source`
- `parser_version`

### 4.3 truth の優先順位

- raw truth: raw WS payload
- normalized truth: Pandera gate 通過済み rows
- execution truth: Nautilus fills / positions / order lifecycle
- live truth: Hyperliquid venue state + user_fills + order_updates

### 4.4 先に決めるべきで、あとで変えないもの

- Hyperliquid only
- isolated only
- Fast is not live-default
- WS primary
- DSR gate
- Pybotters / Nautilus / hftbacktest / DuckDB / Pandera の責務分離

---

## 5. 実装・検証・運用の境界

### 5.1 実装の境界

collector は「データを受けて保存する」までである。
quality gate を持つが、edge を判断しない。

### 5.2 研究の境界

研究は normalized / derived を使ってよいが、raw を直接 strategy に食わせない。

### 5.3 backtest の境界

current engine は strategy logic の粗い評価、Nautilus は execution-aware な truth、hftbacktest は calibration であり、用途を混ぜない。

### 5.4 運用の境界

paper と tiny-live と live は、同じ signal でも venue semantics が違う可能性を持つ。したがって compare report を標準で出す。

---

## 6. ハイブリッドアーキテクチャ追補（最新版の決定事項を反映）

以下は、完成品アーキテクチャ仕様書 v3 の全文を、この資料の附属規範として収録する。矛盾がある場合は 1章と2章が優先される。

---

# StateStrike 完成品アーキテクチャ仕様書 v3

- 作成日: 2026-04-22 JST
- ステータス: 採用版
- 対象: StateStrike Production v2
- 方針: Hyperliquid 専用 / Perpetuals 専用 / isolated only / Core + Re-entry を live 本線 / Fast は research・paper 限定

---

## 1. 目的

本仕様書の目的は、StateStrike の完成品アーキテクチャを固定し、実装・検証・運用・将来拡張の境界を曖昧さなく定義することである。

本書で固定するのは主に次の点である。

1. 利用 OSS / API の責務分離
2. 真実源（source of truth）の定義
3. データ取得・正規化・監査・補完の責務
4. execution-aware backtest と calibration の責務
5. live / paper / backtest の境界
6. 完成品としての必須要件

本書は「実装順のメモ」ではなく、完成品の設計基準である。

---

## 2. スコープ

### 2.1 対象

- 取引所: Hyperliquid のみ
- 商品: USDC 建て Perpetuals のみ
- モード:
  - research
  - backtest
  - paper
  - tiny live
  - live

### 2.2 非対象

以下は本仕様書の完成品スコープから外す。

- GRVT / Bitget / MEXC など他 venue への本番対応
- Spot 戦略
- 現物裁定 / funding carry を主戦略にする構成
- HFT / MM / アービトラージ主軸
- cross margin 常用
- dashboard の豪華化
- ML 主戦略化
- P2 / P3 の本線化

### 2.3 本線戦略

live 本線は次の 2 レーンのみとする。

- Core
- Re-entry

Fast は paper / research でのみ許可し、live 本線には含めない。

---

## 3. 設計原則

### 3.1 source of truth を分ける

StateStrike では truth を 1 つにしない。責務ごとに分ける。

- data truth: Pybotters collector + Hyperliquid WS
- venue truth: Hyperliquid 公式 API / official semantics
- execution truth: NautilusTrader
- queue/latency calibration truth: hftbacktest
- research truth: normalized + derived + audit outputs

この分離により、collector の正しさ、約定の正しさ、queue 仮説の正しさを混同しない。

### 3.2 WS 主 / REST・S3 従

履歴や snapshot だけでは高精度検証に必要な板変化を再現できないため、主経路は WebSocket とする。
REST と S3 は以下に限定する。

- 起動直後 bootstrap
- gap repair
- sanity check
- collector 開始前の補助 backfill

### 3.3 raw と normalized を分ける

collector が受け取った payload は raw として保存する。
同時に、downstream で使うために normalized へ変換する。

raw を残す理由は以下。

- parser バグ修正時の再パース
- 監査
- venue 仕様変化の追跡
- backtest / paper / live 差異の再現

### 3.4 schema gate を前段に置く

収集できたことと、下流に流してよいことは別である。
そのため normalized の入口に schema gate を置き、列・型・時刻整合・値域を検証する。

### 3.5 戦略より先に品質を固める

signal の改善は、次が整ってから行う。

- 欠損監査
- timestamp 整合
- live 実コスト監査
- execution-aware backtest
- multiple testing 補正

---

## 4. 採用 OSS / API と責務

### 4.1 Pybotters

役割:
- 主 collector
- 認証付き / 認証不要 WebSocket 管理
- DataStore 管理
- Hyperliquid の market / user stream 取得

採用理由:
- 既存運用との整合
- HyperliquidDataStore が存在
- async collector 実装をフルスクラッチする必要がない

主責務:
- `l2_book`
- `trades`
- `active_asset_ctx`
- `candle`
- `bbo`
- `all_mids`
- `order_updates`
- `user_fills`
- `user_events`
- `user_fundings`

非責務:
- execution-aware backtest
- queue calibration
- gap audit の判断
- quality gate
- Nautilus export

### 4.2 Hyperliquid 公式 WS / Info / S3

役割:
- venue truth
- gap repair
- sanity check
- official semantics の基準
- optional backfill

主責務:
- subscription semantics
- snapshot semantics
- funding / mark / oracle / OI の意味
- official rate limits / limits

非責務:
- 主 collector
- 長期の完全履歴保証

### 4.3 NautilusTrader

役割:
- execution-aware backtest truth
- L2/L3 order book ベース fill 再現
- fee / fill / partial fill / position / order lifecycle 再現

主責務:
- Core lane の本線評価
- Re-entry lane の本線評価
- paper/tiny-live との差異縮小

非責務:
- raw collector
- queue pessimistic/optimistic のレンジ校正

### 4.4 hftbacktest

役割:
- queue / latency / maker fill probability の校正

主責務:
- ALO の fill band
- IOC rescue の latency/cost band
- Re-entry / soft-exit の queue sensitivity

非責務:
- 主バックテスター
- execution truth
- market impact 再現

### 4.5 DuckDB + Parquet

役割:
- normalized / derived の分析 spine
- audit / compare / report 基盤

主責務:
- cost surface
- regime edge table
- gap audit
- timestamp skew audit
- realized cost compare
- walk-forward 集計

### 4.6 Pandera

役割:
- normalized / derived の schema gate

主責務:
- column presence
- dtype coercion
- strict mode
- ordered mode
- value checks
- custom checks

### 4.7 Tardis（optional）

役割:
- collector 開始前の長期履歴 bootstrap
- long history replay

採用条件:
- 2024-10-29 以降の長期履歴が必要
- すぐに multi-month backtest を始めたい
- official S3 の欠損や timeliness の弱さを補いたい

非採用条件:
- collector 開始以降の自前収集中心で足りる
- 外部ベンダーコストを増やしたくない

---

## 5. 取るべきデータ

### 5.1 最優先

#### 5.1.1 l2_book

用途:
- execution-aware backtest
- spread / depth / impact
- queue calibration 入力
- liquidity regime 推定

#### 5.1.2 trades

用途:
- OFI
- aggressor flow
- trade imbalance
- short-horizon flow persistence

#### 5.1.3 active_asset_ctx

用途:
- markPx
- oraclePx
- funding
- openInterest
- midPx
- basis / mark-oracle divergence
- funding/OI quality audit

### 5.2 補助

#### 5.2.1 candle

用途:
- sanity check
- 1m / 5m / 15m 再構築比較
- rough replay fallback

#### 5.2.2 bbo / all_mids

用途:
- lightweight monitoring
- heartbeat sanity
- BBO quality compare

### 5.3 live 監査

#### 5.3.1 order_updates

用途:
- order lifecycle
- rejected / canceled / replaced 監査

#### 5.3.2 user_fills

用途:
- realized cost audit
- fill price / fee / fill fragmentation

#### 5.3.3 user_events / user_fundings

用途:
- live account state 監査
- funding settlement 追跡

---

## 6. 保存層

### 6.1 raw 層

パス例:

```txt
raw_ws/
  exchange=hyperliquid/
    channel=l2_book/
    channel=trades/
    channel=active_asset_ctx/
    channel=candle/
    ...
```

特徴:
- venue payload を可能な限りそのまま保持
- compression 許可
- parser 変更後の再パース源

### 6.2 normalized 層

パス例:

```txt
normalized/
  l2_book/
  trades/
  asset_ctx/
  candle/
  user_fills/
  order_updates/
```

フォーマット:
- Parquet
- partition by date / symbol / channel

### 6.3 derived 層

- bars
- flow metrics
- cost metrics
- value area proxies
- lane signals
- audit outputs

### 6.4 report 層

- cost_surface
- regime_edge_table
- oi_funding_quality_report
- compare_current_vs_nautilus
- compare_nautilus_vs_live
- dsr_report

---

## 7. canonical columns

### 7.1 全 normalized 共通

最低限、次を持たせる。

- `exchange`
- `symbol`
- `channel`
- `exchange_ts`
- `recv_ts`
- `capture_session_id`
- `parser_version`
- `raw_file_ref`
- `source`

### 7.2 trades

- `px`
- `qty`
- `side`
- `trade_id`（あれば）
- `aggressor_side`（取れる場合）

### 7.3 l2_book

- `side`
- `px`
- `qty`
- `level`
- `is_snapshot`
- `book_epoch`

### 7.4 asset_ctx

- `mark_px`
- `oracle_px`
- `funding_rate`
- `open_interest`
- `mid_px`

### 7.5 user_fills

- `client_order_id`
- `venue_order_id`
- `side`
- `px`
- `qty`
- `fee`
- `fee_asset`
- `trade_ts`

---

## 8. schema gate

normalized 入口に Pandera を置く。

### 8.1 必須ルール

- 必須列存在
- 型 coercion 成功
- `exchange_ts <= recv_ts + allowed_clock_skew`
- qty > 0
- px > 0
- duplicate primary key 不許可（channel 別定義）
- unexpected columns 不許可（strict）

### 8.2 quarantine

schema 失敗行は quarantine へ送る。
正常系と混ぜない。

### 8.3 fail policy

- raw は捨てない
- normalized には入れない
- alert を残す
- audit に記録する

---

## 9. collector の実装原則

### 9.1 WebSocket 主体

collector は WebSocket を主とする。REST polling は collector の主手段にしない。

### 9.2 reconnect 前提

- exponential backoff
- reconnect epoch 採番
- snapshot clear + rebuild
- reconnect 後の gap audit

### 9.3 idempotency

- duplicate message 耐性
- snapshot 再受信耐性
- partial restart 耐性

### 9.4 subscription budget

Hyperliquid の接続数・購読数・メッセージ数制約を超えないよう、collector は symbol batch と channel batch を管理する。

### 9.5 manifest

各 capture batch に対し以下を残す。

- started_at
- ended_at
- symbols
- channels
- ws_disconnect_count
- reconnect_count
- files_written
- row_count
- gap_flags

---

## 10. quality audit

### 10.1 timestamp audit

見る項目:
- `recv_ts - exchange_ts`
- channel 別 skew 分布
- reconnect 前後の skew 変化

### 10.2 gap audit

見る項目:
- expected cadence vs actual cadence
- reconnect 前後の book epoch 切替
- channel 別欠損

### 10.3 book sanity

見る項目:
- best bid < best ask が崩れていないか
- negative qty / zero qty
- same level duplication
- top-of-book gap anomaly

### 10.4 asset_ctx audit

見る項目:
- mark/oracle divergence
- funding 更新タイミング
- OI 更新粒度
- missing ctx periods

### 10.5 live audit

見る項目:
- fill without intent
- intent without fill
- orphan orders
- missing bracket
- realized cost gap

---

## 11. Hyperliquid 公式補完の扱い

### 11.1 Info endpoint

用途:
- bootstrap snapshot
- gap repair
- candle sanity check
- cross-check

制約:
- time range は 500 要素または distinct blocks 単位
- `candleSnapshot` は最新 5000 本まで
- `l2Book` snapshot は片側 20 levels

### 11.2 official S3

用途:
- optional backfill
- cross-check
- collector 開始前の参考履歴

前提:
- 月次更新相当
- timely update 保証なし
- 欠損可能性あり

### 11.3 node_fills data

必要なら fills 系補助データとして利用可。ただし canonical source は live user_fills を優先する。

---

## 12. Nautilus bridge

### 12.1 役割

normalized データを Nautilus の data model へ変換する。

### 12.2 変換対象

- `trades` -> trade ticks
- `l2_book` -> L2_MBP events
- `asset_ctx` -> supplemental side stream
- instrument metadata -> Nautilus instrument definitions

### 12.3 instrument mapping

Hyperliquid perp symbol を Nautilus InstrumentId に変換する。
例:
- `BTC` -> `BTC-USD-PERP.HYPERLIQUID`

### 12.4 venue config

最低限固定する。

- maker fee
- taker fee
- tick size
- lot size
- liquidity_consumption
- oms type
- position accounting

### 12.5 order book constraints

Nautilus Hyperliquid adapter は 1 trader instance あたり 1 instrument 1 order book 制約を持つ。したがって multi-instrument 実行時の instance 分割方針を別途固定する。

### 12.6 execution truth の定義

StateStrike では次を execution truth と定義する。

- fill path
- fee path
- partial fill behavior
- bracket behavior
- cancel / replace behavior

これらの primary evaluator は Nautilus とする。

---

## 13. hftbacktest calibration

### 13.1 役割

hftbacktest は calibration 専用であり、主バックテスターではない。

### 13.2 見る対象

- ALO entry
- IOC rescue
- Re-entry maker fill probability
- soft-exit maker/taker switch timing

### 13.3 latency bands

最低 3 バンド持つ。

- optimistic
- base
- pessimistic

### 13.4 queue bands

最低 3 バンド持つ。

- optimistic queue advancement
- neutral
- pessimistic / risk-averse

### 13.5 出力

- maker fill probability
- avg entry delay
- rescue trigger frequency
- cost uplift band

### 13.6 非責務

- final PnL truth
- final position truth
- market impact truth

---

## 14. Tardis bootstrap

### 14.1 目的

collector 開始前の長期履歴を即時に使えるようにする。

### 14.2 導入条件

- multi-month / multi-quarter の backtest を今すぐ回したい
- official S3 だけでは不足
- 自前収集開始から待てない

### 14.3 利用範囲

- historical replay
- pre-collector bootstrap
- book/trade/ctx の長期比較

### 14.4 canonical integration

Tardis 取り込み時も normalized canonical schema に合わせる。
Tardis 専用 schema を下流へ流さない。

---

## 15. StateStrike 戦略との接続

### 15.1 live 本線

- Core
- Re-entry

### 15.2 research only

- Fast
- post-liquidation experimental sleeve

### 15.3 必要 feature

collector 基盤が供給すべき feature material は次。

- 15m state inputs
- 5m lane inputs
- 30s primary flow
- 60s persistence
- mark/oracle basis
- funding distance
- open interest delta
- spread / impact / depth / refill
- realized cost audit support

### 15.4 OI の扱い

OI は main alpha ではなく confirmation / score modifier / state quality check に留める。

### 15.5 funding の扱い

funding は alpha ではなく veto / size-down / add-on ban に使う。

---

## 16. acceptance / promotion gates

### 16.1 data promotion

raw -> normalized
条件:
- schema pass
- timestamp pass
- duplicate rules pass

### 16.2 normalized -> derived

条件:
- required channels complete
- audit severe issue なし

### 16.3 derived -> backtest

条件:
- required date range completeness
- symbol completeness above threshold
- cost fields available

### 16.4 backtest -> paper

条件:
- Nautilus execution-aware positive median expectancy
- DSR positive
- regime-wise collapse なし

### 16.5 paper -> tiny live

条件:
- paper 21 日重大障害なし
- realized cost assumptions plausible
- bracket / reconciliation stable

### 16.6 tiny live -> live

条件:
- tiny live 14 日重大障害なし
- realized_total_cost in expected band
- duplicate / orphan / missing-bracket zero

---

## 17. acceptance tests

最低限必要な acceptance test は以下。

### 17.1 collector

- disconnect / reconnect 後に capture 継続
- duplicate payload 耐性
- gap repair 動作
- manifest 出力

### 17.2 schema gate

- invalid rows quarantine
- strict column enforcement
- dtype coercion behavior

### 17.3 export

- normalized -> Nautilus conversion
- normalized -> hftbacktest conversion
- symbol mapping correctness

### 17.4 live audit

- fill without intent 検知
- missing bracket 検知
- realized cost report 生成

### 17.5 strategy integration

- Core lane replay
- Re-entry lane replay
- veto states reflected in data

---

## 18. non-functional requirements

### 18.1 再現性

- collector 再起動後も manifest から再現可能
- raw から normalized を再生成可能
- normalized から derived を再生成可能
- versioned parser / schema を持つ

### 18.2 監査可能性

- raw 参照可能
- parser version 追跡可能
- audit reports 保存
- fill/intent 整合追跡可能

### 18.3 拡張性

将来追加可能にする。

- Tardis bootstrap
- additional venues
- additional lanes
- ML veto

ただし v2 完成条件には含めない。

### 18.4 運用性

- restart-safe
- reconnect-safe
- state recovery
- backfill hooks
- alert hooks

---

## 19. 完成条件

### 19.1 データ基盤完了

- Pybotters collector が 24/7 稼働
- raw / normalized / derived の再生成可能
- Pandera gate 稼働
- DuckDB audit 稼働

### 19.2 backtest 基盤完了

- Nautilus export 完成
- Core / Re-entry の execution-aware backtest 稼働
- hftbacktest calibration band 稼働

### 19.3 live 監査完了

- order intent vs fills 突合
- realized cost audit
- orphan / missing bracket 検知

### 19.4 validation 完了

- walk-forward
- DSR
- trial registry
- compare current vs Nautilus vs live

---

## 20. 私たちの最終推奨

### 強く推奨

- Pybotters を主 collector にする
- Hyperliquid WS を主ソースにする
- Info / S3 は補助に留める
- NautilusTrader を execution truth にする
- hftbacktest は calibration 専用にする
- DuckDB + Parquet を分析 spine にする
- Pandera を入口の schema gate にする
- Tardis は必要時だけ追加する
- Core + Re-entry を live 本線にする
- Fast は live に入れない

### 非推奨

- Pybotters 単独完結
- REST / S3 主体設計
- hftbacktest 主バックテスター化
- quality gate なし downstream
- Fast の live 本線化
- OI の主シグナル化

---

## 21. 次の実装順

1. Pybotters collector 正式化
2. Pandera gate 実装
3. DuckDB audit 基盤実装
4. Hyperliquid gap repair / sanity hooks 実装
5. Nautilus export 実装
6. Core lane Nautilus strategy 実装
7. Re-entry Nautilus strategy 実装
8. hftbacktest calibration export 実装
9. compare reports 実装
10. trial registry + DSR 実装

---

## 22. 最終要約

StateStrike の完成品アーキテクチャは、collector から backtest、paper、tiny live までを 1 つの巨大ツールで解く設計ではない。

- **Pybotters** が collector truth
- **Hyperliquid 公式仕様** が venue truth
- **NautilusTrader** が execution truth
- **hftbacktest** が queue/latency calibration truth
- **DuckDB + Pandera** が品質と監査の spine
- **Tardis** は長期履歴が必要な時だけ使う optional bootstrap

この責務分離こそが、実装は重くても再現性を高め、後から戦略を Better にし続けられる基盤である。


---

## 付録A. 完成品仕様書（基準版・全文収録）

以下は、完成品仕様書 v1.3 の全文収録である。本文で矛盾解消済みの部分は本文を優先する。

---


# StateStrike Production Specification v1.3
## 完成品仕様書（Hyperliquid / Perpetuals / Core + Re-entry / Execution-Aware）

文書番号: SS-PROD-SPEC-v1.3  
文書種別: 完成品仕様書  
作成日: 2026-04-21 JST  
対象読者: オーナー、シニアSE、実装担当、検証担当、運用担当  
対象成果物: 完成品としての StateStrike 本体、研究・検証基盤、paper/tiny-live/live 運用系、監査系  
本文の位置づけ: MVPではなく、完成品として何を作るか、何を作らないか、どこまでを完成条件とみなすかを規定する基準文書

---

## 0. この文書の読み方

この文書は、単なる機能一覧ではない。  
完成品としての StateStrike を、**戦略・システム・運用・監査・検証**の全レイヤで固定するための文書である。  
したがって、読者は最初に次の前提を受け入れる必要がある。

1. 本文書は「儲かる保証」を与えるものではない。  
   ここで定義するのは、**live で edge が残る可能性を最大化するための完成品の仕様**である。
2. 本文書は、プロジェクト固定要件と、これまでに回収済みの戦略思想、そして最新の公式ドキュメントおよび研究知見を統合したものである。
3. 本文書は、実装前に設計を固定するためのものであり、**仕様変更には変更管理が必要**である。
4. 本文書において、  
   - 「固定」と書かれたものは実装側が任意に変えてはならない  
   - 「設定値」と書かれたものは config で変更可能  
   - 「研究仮説」と書かれたものは validation gate を通過しない限り本番既定値にしてはならない  
5. 本文書は MVP の範囲を越える。  
   したがって、paper/tiny-live/live、execution-aware backtest、監査、再起動耐性、incident runbook まで含む。

この文書を読む順序は次を推奨する。  
1章から5章でプロダクトの輪郭を押さえ、6章から13章でアーキテクチャを理解し、14章から23章で戦略と執行を把握し、24章以降で運用・監査・完成条件を確認する。  
Appendix は実装者がそのまま写経できる粒度の補助資料である。

---

## 1. 目的

StateStrike の目的は、**個人の資本規模とインフラ条件のもとで、暗号資産 perpetual 市場における短中期の条件付き edge を、after-cost で現実に回収できる自動売買システムを構築すること**である。

ここで重要なのは、目的を「高い paper ROI を示すこと」に置かない点である。  
小資金では、見かけの ROI が高くても、約定数が少なすぎたり、再現性が低かったり、実コストで崩れたりすれば意味がない。  
本システムは、次の 4 条件を同時に満たすことを目指す。

- **再現性**: backtest / paper / tiny-live / live の間で挙動差が説明可能であること
- **収益性**: after-cost の期待値が正になる可能性を持つこと
- **運用可能性**: 個人の物理サーバー環境で 24/7 運用できること
- **破綻回避**: isolated margin、固定 fractional risk、DD stop により損失上限を制御できること

言い換えると、StateStrike の目的は「すべての相場で無理に戦うこと」ではない。  
**edge がある state に限定して戦い、edge がない state では何もしない**ことが設計思想の中心である。

---

## 2. 適用範囲

### 2.1 対象市場
- 対象商品は **暗号資産 perpetual contracts のみ**
- 対象会場の完成品 v1.3 における本番サポートは **Hyperliquid のみ**
- GRVT、他会場、CEX 現物、オプション、現物裁定は本完成品の対象外
- spot は oracle / value / basis 参照のために間接的に使うことはあっても、売買対象にはしない

### 2.2 対象戦略
完成品 v1.3 で本番稼働を許可する戦略は次のみ。

- **Core lane**: Liquid Auction Transition Continuation
- **Re-entry lane**: 同一 state 内の浅い押し/戻り continuation
- **Fast lane**: 実装してよいが、完成品の本番既定値では無効。paper 研究専用
- **Post-Liquidation Recovery**: 研究対象。完成品 v1.3 の本番既定値では無効

### 2.3 非対象
以下は本仕様の完成品範囲から明示的に除外する。

- HFT
- マーケットメイキングを主収益源とする戦略
- venue 間裁定を主収益源とする戦略
- ML を主シグナルにする設計
- cross margin 運用
- 高頻度シグナルの量産のみを目的とした low-quality entry
- 実装容易性だけを優先して execution fidelity を犠牲にする backtest

---

## 3. 完成品の定義

### 3.1 完成品とは何か
本仕様における「完成品」は、次の一文で定義する。

**StateStrike Production v1.3 とは、Hyperliquid 上で、Core と Re-entry の 2 レーンを isolated 限定で 24/7 自律運用でき、execution-aware backtest・paper・tiny-live・live・risk guard・realized cost audit・incident recovery・validation gate を備えた、小資金向け perpetual 自動売買システムである。**

この定義の重要点は次の通り。

- 完成品であっても、利益保証はしない
- 完成品は「一通り動くもの」ではなく、**本番運用で壊れにくいこと**を含む
- 完成品は strategy engine だけでなく、**data / execution / audit / ops** を含む
- 完成品と増額許可は別である

### 3.2 完成品と増額許可の違い
完成品は「システムとして運用に足る状態」を意味する。  
一方で増額許可は、「より大きな資金を入れてもよい」と判断するための別ゲートである。

#### 完成品の最低条件
- paper 21 日連続で重大障害なし
- tiny-live 14 日連続で重大障害なし
- stop 未付与・duplicate unintended position・orphan position 放置が 0 件
- execution-aware backtest と tiny-live の差が説明可能
- DSR と walk-forward gate を通過

#### 増額許可の最低条件
- tiny-live 8 週間以上
- 100 closed trades 以上
- after-cost net positive
- realized cost の分布が想定帯から大きく逸脱していない
- manual intervention が例外的

### 3.3 完成品に含まれないもの
完成品 v1.3 において、次は optional / future work であり、完成の定義に含めない。

- Web UI ダッシュボード
- マルチ会場自動切替
- portfolio allocator
- ML veto / size-up
- Post-Liquidation Recovery の live 本番化
- 完全自動最適化

---

## 4. 証拠ランクと仕様の拘束力

この文書では、各要素の性格を 4 段階で区別する。

### 4.1 [固定]
変えてはならない前提。
例:
- perpetual のみ
- Hyperliquid 本番
- isolated only
- HFT/MM/裁定主戦略を採らない
- 74h 変化率と出来高増による universe 絞り込み
- signal より execution/risk を重視すること

### 4.2 [仕様決定]
設計上の判断として固定するもの。  
例えば:
- 15m state / 5m lane / 30s flow / 60s persistence
- Core + Re-entry 本番、Fast は paper 専用
- NautilusTrader を execution truth に採用
- hftbacktest は calibration 専用
- DSR を validation gate に含める

### 4.3 [設定値]
config で変更可能だが、運用・検証の承認を要するもの。  
例:
- `risk_fraction`
- `edge_to_cost_min`
- `minutes_to_next_funding` の veto 閾値
- `impact_bps` 閾値
- lane ごとの size multiplier

### 4.4 [研究仮説]
production default にする前に検証が必要なもの。  
例:
- value migration 優位の強さ
- micro-POC を用いた re-entry の優位
- add-on 1/2 の本番常時有効化
- certain order-flow proxy の有効性

この区別を曖昧にすると、実装側が勝手に設計意図を変えてしまう。  
とくに、**仕様決定**と**設定値**と**研究仮説**を混同してはならない。

---

## 5. デザイン原則

StateStrike の完成品は、次の原則で設計する。

### 5.1 原則1: 価格より execution
価格パターンの見た目よりも、**実際にそのサイズで、どの order style なら、どれだけのコストで入れるか**を重視する。  
paper で良くても tiny-live で崩れる戦略は不採用とする。

### 5.2 原則2: すべての相場で戦わない
edge のない state に入らないことは、entry skill の一部である。  
シグナル数不足を、条件を雑に緩めることで解決してはならない。

### 5.3 原則3: 主役は order flow / liquidity / cost
古典的 breakout そのものを alpha の源泉とみなさない。  
alpha 候補は、**auction transition, order-flow persistence, liquidity-conditioned continuation, edge-to-cost** の組み合わせから生まれると考える。

### 5.4 原則4: OI と funding は主シグナルではない
OI は品質問題がありうるため、主シグナルではなく confirmation / veto / score modifier に留める。  
funding は continuation を支持する時もあるが、funding settlement 近傍では market quality が悪化しやすいため、size-down または veto の対象にする。

### 5.5 原則5: 戦略と研究を分離する
Fast lane や post-liquidation playbook のように、魅力的だが安定性が十分でない要素は、paper / research に留める。  
完成品の本番レベルに上げるには、別の validation gate を通す。

### 5.6 原則6: 再現性のない改善は改善ではない
feature を増やした結果 Sharpe が上がっても、trial count や multiple testing を考慮しなければ、その改善は信頼しない。  
DSR と walk-forward median を必須ゲートにする。

---

## 6. 完成品のシステム全体像

StateStrike 完成品は、以下の 7 層で構成する。

1. **Raw Data Layer**  
   Hyperliquid からの trades / l2Book / asset context / fills / order updates / open orders / clearing state を収集する層
2. **Canonical Data Layer**  
   raw を venue 依存から切り離し、Parquet + SQLite に格納する層
3. **Derived Feature Layer**  
   bars, profiles, OFI, cost, state に必要な feature を生成する層
4. **State & Signal Layer**  
   universe, tradability bucket, state engine, lane router, score を生成する層
5. **Execution Layer**  
   ALO / IOC / trigger / bracket / reconciliation / schedule cancel を扱う層
6. **Risk & Guard Layer**  
   sizing, DD stop, cooldown, stale feed kill switch, isolated enforcement を扱う層
7. **Validation & Audit Layer**  
   backtest, paper, tiny-live, realized cost audit, DSR gate, incident reporting を扱う層

この 7 層のどれかを欠くものは、完成品とは呼ばない。  
特に、strategy code だけで「完成」とみなすことを禁止する。

---

## 7. 主要 OSS / 研究の採用方針

### 7.1 NautilusTrader
役割: **execution-aware backtest の truth source**  
採用理由:
- Hyperliquid integration がある
- L2/L3 book ベースで market/limit/stop/partial fill を再現できる
- live/backtest parity を作りやすい
- `liquidity_consumption` を持ち、同一板流動性の二重利用を避けられる

非役割:
- strategy ideation の高速総当たり
- queue pessimism の校正

### 7.2 hftbacktest
役割: **queue / latency / maker fill probability の calibration**  
採用理由:
- queue model を選べる
- latency model を持てる
- maker order の刺さりやすさの悲観/標準/楽観レンジを作れる

注意:
- market-data replay 型であり、市場インパクトを再現しない
- 主 engine にしてはならない

### 7.3 Hyperliquid Python SDK
役割: **live fallback / ops / signing 補助**  
採用理由:
- 公式 SDK
- live 注文や署名面での保守性

非役割:
- 主 backtesting engine
- strategy logic の truth source

### 7.4 MarketProfile 系
役割: **value migration 仮説の試作・比較**  
採用理由:
- VAH/VAL/POC/LVN/HVN を素早く試せる

注意:
- production の主依存にしない
- 採用 features は自前実装に移す

### 7.5 DSR / PBO 系
役割: **採用ゲート**  
採用理由:
- multiple testing で膨らんだ見かけの成績を補正するため

---

## 8. 取引所前提: Hyperliquid

完成品 v1.3 は Hyperliquid を唯一の本番会場とする。  
本仕様では、Hyperliquid の次の事実を前提にする。

- perpetual は funding 支払いで価格を spot に近づける
- funding は **1時間ごと**
- premium は **5秒ごとの impact/oracle 差サンプル**を用いて計算される
- oracle と mark はおおむね **3秒ごと**
- mark price は margining / liquidations / TP/SL に使われる
- order book は **price-time priority**
- order types と options として、market / limit / stop / take / scale / TWAP、GTC / ALO / IOC / reduce-only などが使える
- cross と isolated があり、完成品では **isolated のみ許可**
- OI cap 到達時には新規ポジション増加が制限され、oracle 近傍の制約も発生する

これらの性質から、StateStrike は次の設計を採る。

- signal 判定には `mark` と `oracle` を明示的に使う
- funding 近傍は veto または size-down の対象にする
- execution は `ALO` 主体、必要時のみ `IOC rescue`
- OI cap 近傍銘柄では continuation を極力避ける
- `mark-trigger` と reconciliation を前提に stop/TP を設計する

---

## 9. 環境前提

### 9.1 ハードウェア
- 個人所有の物理サーバー
- high-end CPU
- 128GB RAM
- 1TB NVMe SSD
- 100MB/s 級回線

### 9.2 OS / runtime
- Ubuntu もしくは Proxmox 配下の Linux
- Python 3.13
- package / env 管理は **uv**
- 必要に応じて Rust バイナリや systemd unit を併用

### 9.3 言語役割
- Python: data, feature, signal, validation, ops
- Rust: 必要なら collector / performance-critical part
- TypeScript / Svelte: UI が必要な将来拡張
- 本完成品 v1.3 の必須スコープに UI は含めない

### 9.4 制約
- 低レイテンシ競争はしない
- 10秒未満のフローは補助用途に留める
- signal の主時間軸は 30s〜60s に置く
- self-hosted, single-operator 運用を前提にする

---

## 10. ランタイムモード

StateStrike 完成品は次の 6 モードを持つ。

### 10.1 Research
特徴量探索、ablation、dataset 生成。  
本番と同じ codebase を使うが、注文は一切出さない。

### 10.2 Execution-aware Backtest
NautilusTrader ベースの backtest。  
Core / Re-entry の execution truth を検証する。  
self backtest より優先。

### 10.3 Calibration
hftbacktest による queue / latency / maker fill band 推定。  
本体ではなく補助。

### 10.4 Paper
リアルデータを使うが、注文は paper state にだけ書く。  
paper router, paper positions, paper fill audit を使う。

### 10.5 Tiny-live
最小ノーション・isolated 小枠で live 注文を出す。  
増額ゲートの前段。  
このモードでは schedule cancel、reconciliation、cost audit が必須。

### 10.6 Live
増額許可後の本番。  
完成品仕様に含まれるが、増額は別 gate とする。

---

## 11. データモデルの原則

### 11.1 Raw first
必ず raw を保存する。  
derived だけを残す構成は禁止。  
理由:
- feature 再計算
- バグ検証
- incident 再現
- 新 feature の後付け計算
ができなくなるため。

### 11.2 Canonical schema
raw をそのまま strategy が読むことを禁止する。  
理由:
- venue 差分が strategy に漏れる
- backtest/live parity が壊れる

### 11.3 Immutable raw, reproducible derived
raw は append-only。  
derived は再生成可能でなければならない。

### 11.4 UTC + monotonic handling
時刻は UTC 基準で管理し、source timestamp, ingest timestamp, process timestamp を分ける。  
paper/live/backtest の再現性のため、ts の定義は厳格にする。

---

## 12. Raw Data Layer 仕様

### 12.1 収集対象
最低限次を収集する。

- trades
- l2Book
- active asset context
- all mids / per-asset mids（必要に応じて）
- user fills
- order updates
- open orders
- clearinghouse state
- account / position relevant snapshots
- meta / instrument metadata
- OI cap / asset static info

### 12.2 保存単位
- 日付 partition
- source type partition
- symbol partition
- raw gzip/snappy Parquet

### 12.3 Raw schema: trades
例:

```txt
ts_event
ts_ingest
venue
symbol
trade_id
price
size
side_aggressor
is_snapshot
raw_json
```

### 12.4 Raw schema: l2Book
```txt
ts_event
ts_ingest
venue
symbol
bid_levels_json
ask_levels_json
best_bid
best_ask
mid
depth_checksum(optional)
raw_json
```

### 12.5 Raw schema: asset context
```txt
ts_event
ts_ingest
venue
symbol
mark_px
oracle_px
funding_rate
open_interest
impact_bid_px
impact_ask_px
next_funding_ts
raw_json
```

### 12.6 Raw schema: user fills
```txt
ts_event
ts_ingest
venue
symbol
fill_id
order_id
client_order_id
price
qty
fee
liquidity_side
raw_json
```

### 12.7 Raw schema: order updates
```txt
ts_event
ts_ingest
venue
symbol
order_id
client_order_id
status
remaining_qty
avg_fill_px
reason
raw_json
```

### 12.8 Collector 要件
- reconnect 必須
- snapshot ack の再処理に耐える
- dedup 必須
- stateful parser 必須
- source side disconnect を前提とする
- missed data は snapshot ack または info query で穴埋めする

### 12.9 Collector 完成条件
- 24h 稼働で重大欠損なし
- reconnect 後の snapshot でデータ連続性が保てる
- high load 時もメモリ leak がない
- SQLite 等の同期書き込みが ingest latency を殺さない

---

## 13. Canonical / Derived Data Layer 仕様

### 13.1 Canonical bars
- 15m mark bar
- 5m mark/trade joined bar
- 30s flow bar
- 60s persistence bar

### 13.2 Canonical join
bar 生成時には、少なくとも次を join する。

- mark
- oracle
- trades
- spread
- top-book depth
- impact
- funding
- OI
- next funding time

### 13.3 Derived feature families
1. universe features
2. cost / tradability features
3. auction transition features
4. order flow features
5. OI/funding features
6. book resilience features
7. value profile features
8. risk / PnL / audit features

---

## 14. Universe Filter 仕様

Universe filter はプロジェクトの固定要件であり、完成品でも維持する。  
定義は次。

- `abs(ret_74h) >= 4%`
- `volume_ratio_24 >= 1.15`

ただし、実装上は次の拡張を許可する。

- `ret_74h` は mark close 由来
- `volume_ratio_24` は trade volume 由来
- lookback と aggregation は config 化
- symbol freeze / delist / isolated-only assets の個別ルールは別に適用

Universe filter は「寝ている銘柄を除外する」ためのものであり、  
この段階では trade cost や flow quality は見ない。  
その役割は後段の tradability gate と state engine に委ねる。

---

## 15. Tradability & Cost Surface

### 15.1 目的
「シグナルが良い」ことと、「今そのサイズで入る価値がある」ことは別問題である。  
Tradability gate は、後者を扱う。

### 15.2 基本指標
- `spread_bps`
- `impact_bps(order_notional)`
- `depth_top3_usd / order_notional`
- `maker_fill_proxy`
- `expected_total_cost_bps`
- `realized_total_cost_bps`（audit 後）

### 15.3 bucket
完成品では次の 3 bucket を持つ。

- `M`: continuation を許可
- `L`: continuation を禁止、研究戦略のみ
- `X`: trade 禁止

### 15.4 初期暫定定義
```txt
M:
  expected_total_cost_bps <= 16
  impact_bps <= 6
  depth_top3_usd / order_notional >= 8

L:
  M に入らないが、極端な流動性不足ではない

X:
  それ以外
```

### 15.5 完成品での本定義
固定閾値だけではなく、条件付き cost surface を持つ。  
少なくとも次の軸で管理する。

- symbol
- time-of-hour
- funding_distance_bucket
- shock_state
- order_style
- order_notional bucket
- lane type

### 15.6 `edge_to_cost`
最終的な executable 判定では、次を基本概念にする。

```txt
edge_to_cost = E[conditional MFE after cost] / expected_total_cost_bps
```

これが所定以上でなければ executable にはしない。  
固定 bps 閾値は safety floor としてのみ使う。

---

## 16. Value Area / Auction Transition

### 16.1 背景
本仕様では、古典的な `recent high / recent low breakout` を alpha の中心にしない。  
代わりに、**旧い価値帯から新しい価値帯へ市場が移行したか**を評価する。

### 16.2 profile source
完成品の本番では自前実装の volume-at-price profile を使う。  
試作段階では MarketProfile 系ライブラリの利用を許可するが、本番依存は禁止。

### 16.3 profile outputs
最低限次を生成する。

- `POC_prev`
- `VAH_prev`
- `VAL_prev`
- `value_mid_prev`
- `micro_poc_6h`
- `lvn/hvn` optional

### 16.4 value area 定義
- セッションは UTC 日次 anchored を標準とする
- profile は trade notional を価格バケットごとに集計
- POC は最大出来高価格帯
- VAH/VAL は POC を中心に累積 70% volume covering band を使う

### 16.5 Lite fallback
raw profile が不足する場合の fallback として、15m mark close rolling quantile による Lite 近似を許可する。  
ただし、本番既定値では **true profile 実装** を使う。

---

## 17. Market State Engine

### 17.1 state 一覧
完成品 v1.3 では、次の state を持つ。

- `BALANCED_VALUE`
- `AT_UP`
- `AT_DN`
- `POST_LIQUIDATION_DISORDER`
- `DISABLED`

### 17.2 `BALANCED_VALUE`
旧い value area 内に価格があり、meaningful displacement がない状態。  
新規 continuation は禁止。

### 17.3 `AT_UP`
旧い value area の上に、意味のある displacement を伴って出て、acceptance 可能性がある状態。  
Long 系 lane の候補。

### 17.4 `AT_DN`
旧い value area の下に、意味のある displacement を伴って出て、acceptance 可能性がある状態。  
Short 系 lane の候補。

### 17.5 `POST_LIQUIDATION_DISORDER`
forced liquidation / cascade 的な挙動が観測され、continuation と同じ評価軸で扱ってはならない状態。  
本番 continuation は禁止。

### 17.6 `DISABLED`
data / guard / ops 上の理由で新規発注禁止。

### 17.7 liquidation shock 判定
最低限、次のような proxy を使う。

```txt
abs(ret_1m) >= 2 * atr_1m
and abs(delta_oi_60s_z) >= 2
and spread_shock_z >= 2
```

実装では符号や venue 特性に応じて調整してよいが、continuation への hard veto であることは固定。

---

## 18. シグナル階層: L1 / L2 / L3

完成品では、シグナルを 3 層に分ける。

### 18.1 L1 Opportunity
「有望な状態が始まった」シグナル。  
件数目標は静穏週でも 42 件以上。  
この層では注文しない。

### 18.2 L2 Tradable
売買候補として成立した状態。  
品質はあるが、まだ最終実行許可ではない。

### 18.3 L3 Executable
実際に注文してよいシグナル。  
risk, tradability, cost, lane score をすべて通る必要がある。

### 18.4 なぜ三層化するか
低資金運用では「何も起きない週」が困る。  
しかし、その解決を final executable signal の粗製乱造で行うとコスト負けしやすい。  
だから、**機会検知の数**と**実注文の数**を分ける。

---

## 19. レーン構造

### 19.1 Fast
- 目的: 早い小口先行
- 本番既定値: 無効
- 許可モード: research / paper
- 特徴: シグナル数は増えるが、execution risk が高い

### 19.2 Core
- 目的: 本線
- 本番既定値: 有効
- 許可モード: backtest / paper / tiny-live / live
- 特徴: acceptance, flow persistence, cost gate を最も厳格に通す

### 19.3 Re-entry
- 目的: 同一 state 内の浅い押し/戻り continuation
- 本番既定値: 有効
- 特徴: 回数増の主役。Core より size は小さいが、Fast より live に向く

### 19.4 lane priority
同一 symbol で同一方向の signal が重なった場合:

1. 既存 Core position がないなら Core を優先
2. 既存 Core position ありなら Re-entry を検討
3. Fast は research/paper 以外では抑止
4. opposing lane は同時保有しない

---

## 20. 時間軸構造

### 20.1 15m
役割: state  
見るもの:
- universe 条件
- value area displacement
- funding proximity
- shock state
- major bucket

### 20.2 5m
役割: lane  
見るもの:
- non-reentry
- shallow pullback
- re-acceptance
- execution window

### 20.3 30s
役割: primary flow  
見るもの:
- OFI
- basis short-term change
- impact recovery
- refill tendency

### 20.4 60s
役割: persistence  
見るもの:
- OFI persistence
- delta OI
- spread regime
- cost stability

### 20.5 10s
役割: optional execution assist only  
本番 signal の必須条件にはしない。  
使う場合も、
- ALO rescue 可否
- soft exit の先行警告
- micro refill 補助
に限定する。

---

## 21. Long / Short の基本対称性

StateStrike は Long 専用でも Short 専用でもない。  
**上方向 state なら Buy / Long、下方向 state なら Sell / Short** で完全対称に扱う。  
ただし、実装上は市場構造上の非対称（funding bias, spot-led moves, venue liquidity asymmetry）がありうるため、score や bucket 通過率は同一であることを要求しない。  
対称なのはルールの構造であり、観測される頻度ではない。

---

## 22. Core lane エントリー仕様

### 22.1 Core Long: L1
次を満たしたら Long opportunity を 1 件と数える。

- universe 通過
- 15m で `AT_UP` 候補
- `mark_close_15m > VAH_prev + displacement_min`
- hard veto 非該当

ここではまだ注文しない。

### 22.2 Core Long: L2
次を追加で満たしたら Long tradable。

- 5m で旧 value area に深く戻らない
- 30s OFI が正
- 60s OFI が正
- `delta_oi_60s >= 0`
- funding 近接 severe ではない
- expected cost が極端に悪くない

### 22.3 Core Long: L3
次を追加で満たしたら Long executable。

- `M bucket`
- `edge_to_cost >= core_min`
- lane score >= core_score_min
- isolated risk budget 空きあり
- DD stop / daily stop / cooldown 非発火
- ALO or IOC policy 決定可能

### 22.4 Core Short
Long の符号を反転したもの。  
境界は `VAL_prev`、flow は負、`delta_oi_60s <= 0`。

### 22.5 displacement_min
暫定定義:

```txt
displacement_min = max(
  0.30 * ATR_15m,
  2.0 * spread_px_5m,
  0.5 * impact_px(order_notional)
)
```

### 22.6 non-reentry
Long なら、
- 次の 1〜2 本で `VAH_prev` 内に戻らない
- retrace は displacement の 40% 以下

Short なら逆。

### 22.7 score
Core score の入力例:
- acceptance quality
- OFI 30s
- OFI 60s
- delta OI
- basis stability
- impact / refill
- funding distance
- shock cleanliness

---

## 23. Re-entry lane エントリー仕様

### 23.1 目的
本線 state が継続している間に、浅い押し目/戻りから再度 continuation を取る。  
回数増の主役。

### 23.2 Re-entry Long: L1
- 直前に `AT_UP` か Long Core 成立あり
- 5m で shallow pullback 開始
- old value area 深部には戻らない

### 23.3 Re-entry Long: L2
- 価格が `VAH_prev`, `micro_poc`, `session_vwap` の近傍
- 30s OFI が再加速
- 60s が大きく逆行していない
- depth/refill が極端に悪くない

### 23.4 Re-entry Long: L3
- `edge_to_cost >= reentry_min`
- lane score >= reentry_score_min
- existing core/reentry exposure cap 未超過
- no hard veto

### 23.5 Re-entry Short
Long の逆。

### 23.6 サイズ
Core より小さくする。  
原則:
- Re-entry size <= 0.7 * Core base size
- 同方向の合計 exposure 上限を超えない

### 23.7 何を re-entry とみなさないか
- old value area 深部への再侵入
- funding 直前の戻り
- shock 直後の戻り
- OFI が再加速していない単なるドリフト

---

## 24. Fast lane 仕様（research / paper only）

### 24.1 目的
state 初期で小さく先行する。  
件数を増やす。

### 24.2 本番既定値
`enable_fast_live = false`

### 24.3 条件
Core より一段早く、
- 15m state 候補
- 5m displacement
- 30s flow positive
- cost が致命的でない
で発火させる。

### 24.4 サイズ
- 0.25x〜0.5x
- add-on 禁止
- time stop 短い

### 24.5 なぜ本番既定値で無効か
fee・spread・queue・missed fill に弱く、静穏週に過剰売買へ寄りやすいため。  
有効化には別の validation gate を通す。

---

## 25. edge-to-cost

### 25.1 定義
`edge_to_cost` は「この条件の signal が、今の予想コストを支払ってもなお残るか」を数値化する。

### 25.2 理想形
```txt
edge_to_cost =
  E[conditional MFE_next_H | state, bucket, lane, score_bucket] /
  expected_total_cost_bps
```

### 25.3 MFE horizon
標準は:
- Core: 6 bars (5m)
- Re-entry: 4 bars (5m)
- Fast: 2〜3 bars (5m)

### 25.4 expected_total_cost_bps
少なくとも次を含む。
- fee
- spread crossing cost
- impact
- rescue order penalty
- anticipated missed maker -> taker fallback penalty

### 25.5 floor
固定 floor も持つ。
- `expected_total_cost_bps` safety max
- `impact_bps` max
- `depth ratio` min

---

## 26. funding / OI / basis の扱い

### 26.1 funding
funding は主シグナルではなく veto / penalty / size-down に使う。  
標準ルール:
- `minutes_to_next_funding <= 10` -> hard veto
- `10 < minutes_to_next_funding <= 30` -> size-down
- `|funding_z| >= severe` -> add-on 禁止または veto

### 26.2 OI
OI は confirmation として使う。  
主シグナルにしてはならない。  
理由:
- venue 品質問題
- update timing ambiguity
- liquidation message 遅延可能性

### 26.3 basis
`mark - oracle` やその変化は、flow quality の補助に使う。  
極端な逆行や不安定化は score 減点、または soft exit 条件。

---

## 27. sizing

### 27.1 大原則
- fixed fractional を主軸
- score-based size-up は補助
- empirical Kelly は cap としてのみ使う
- isolated only
- no top-up during live

### 27.2 base risk fraction
lane ごとに別定義。
- Core
- Re-entry
- Fast

### 27.3 size formula
概念式:

```txt
risk_dollar = sleeve_equity * lane_risk_fraction * lane_size_mult * score_mult
qty = risk_dollar / stop_distance
qty = min(qty, empirical_kelly_cap_qty, venue_max_qty, sleeve_exposure_cap_qty)
```

### 27.4 stop_distance
- ATR-based
- acceptance invalidation level
- lane minimum stop
の最大値を使う。

### 27.5 empirical Kelly cap
Kelly は過大化しやすいため、
`0.25 * empirical_kelly_size`
を上限 cap とする。  
主 sizing driver にしない。

---

## 28. add-on

### 28.1 完成品での位置づけ
機能としては完成品に含む。  
ただし default は conservative とする。

### 28.2 Core add-on 1
- `unrealized_r >= 0.5`
- flow persistence 継続
- impact deterioration 悪化なし
- funding 近接でない

### 28.3 Core add-on 2
- `unrealized_r >= 1.0`
- OI acceleration が悪くない
- basis 不安定化なし
- funding まで余裕あり

### 28.4 Re-entry とは違う
add-on は同じ建玉への増し玉。  
Re-entry は別シグナル・別発注単位。

### 28.5 本番 default
- add_on_1 enabled
- add_on_2 feature flag
- Fast add-on 禁止

---

## 29. exit

### 29.1 hard exit
- stop loss
- stale feed
- DD stop / daily stop
- venue/account inconsistency severe
- orphan / reconciliation failure

### 29.2 soft exit
Long 例:
- OFI decay
- delta OI decay with no new high
- basis adverse move
- edge_to_cost collapse
- re-entry fail
- funding <= 10 min
- old value area reentry

Short は逆。

### 29.3 time exit
Fast / Re-entry では特に重要。  
長く引っ張らない。

### 29.4 lane ごとの違い
- Fast: 速く切る
- Core: 標準
- Re-entry: micro-POC / VWAP を重視し、Core より早く切る

---

## 30. 注文執行方針

### 30.1 基本方針
- maker-first
- rescue limited
- bracket mandatory
- reduce-only for exits
- isolated only

### 30.2 ALO
最優先。  
ただし `BadAloPx` や明白な missed entry には対応する。

### 30.3 IOC rescue
条件が依然として有効で、逃したコストより獲得 edge が大きい場合のみ。

### 30.4 market order
原則禁止。  
例外は system safety / urgent flattening / liquidation avoidance のみ。

### 30.5 TP/SL
mark-trigger を前提に扱う。  
TP/SL は execution layer で管理し、orphan bracket を許さない。

### 30.6 schedule cancel
dead-man’s switch として定期的に更新。  
cancel reservation が失敗したら新規 entry 禁止。

---

## 31. Order Lifecycle & Reconciliation

### 31.1 identifiers
- deterministic `client_order_id`
- venue `order_id`
- parent-child relation for brackets/add-ons

### 31.2 submit path
- signal -> intent -> router policy -> order submission -> ack -> open order -> fill -> position update -> audit

### 31.3 reconciliation sources
- orderUpdates
- userFills
- openOrders
- clearinghouseState
- fallback info polling

### 31.4 incident classes
- duplicate open entry
- duplicate open position
- missing brackets
- stale pending entry
- fill without intent
- intent without venue trace
- orphan open order
- orphan position

### 31.5 policy
critical incident 発生時は symbol freeze または global safe stop に入る。

---

## 32. Paper Mode

### 32.1 目的
logic と execution policy を live data 上で検証する。

### 32.2 必須機能
- deterministic order IDs
- maker / IOC rescue
- bracket attach
- paper fills
- paper positions
- fill audit
- incident journal

### 32.3 完成条件
- 21 日連続で重大障害なし
- signal -> intent -> fill -> position -> exit が一貫
- backtest と差分が説明可能

---

## 33. Tiny-live Mode

### 33.1 目的
最小ノーションで live discrepancy を測る。  
収益最大化ではなく、execution truth の測定。

### 33.2 必須条件
- isolated only
- very small sleeve
- schedule cancel
- user stream reconciliation
- realized cost audit
- add-on feature flag conservatively set
- automatic incident logging

### 33.3 禁止事項
- top-up
- Fast lane live on
- cross margin
- manual discretionary overrides without journal

### 33.4 完成条件
- 14 日連続で重大障害なし
- realized cost gap が説明可能
- orphan / duplicate / missing stop 0

---

## 34. Live Mode

### 34.1 前提
増額許可後のみ。

### 34.2 追加要件
- backup/restore tested
- systemd supervised services
- alerting
- runbook completed
- weekly review

### 34.3 live でも変わらないもの
- isolated only
- Core + Re-entry only
- hard veto strict
- DSR gate maintained

---

## 35. Audit / Monitoring

### 35.1 必須監査
- realized cost audit
- fill audit
- order intent audit
- paper vs live discrepancy report
- lane contribution report
- funding distance report
- symbol bucket report

### 35.2 主要メトリクス
- gross pnl
- net pnl after cost
- realized_total_cost_bps
- expected_total_cost_bps
- cost_gap_bps
- win rate
- avg win / avg loss
- MFE / MAE
- add-on contribution
- lane contribution
- state contribution
- incident count
- missed entry count

### 35.3 daily report
日次で少なくとも以下を出す。
- realized pnl
- open risk
- fill distribution
- cost distribution
- incidents
- funding proximity stats
- symbols traded

---

## 36. Validation Methodology

### 36.1 why
よく見える戦略を作ることではなく、**live に近い条件で残る戦略だけを採用する**ため。

### 36.2 layers
- self backtest
- Nautilus execution-aware backtest
- hftbacktest calibration
- paper
- tiny-live
- live

### 36.3 acceptance ordering
Nautilus > self backtest  
tiny-live > paper  
realized > expected

### 36.4 required studies
- cost surface
- regime edge table
- OI/funding quality report
- queue/fill calibration
- DSR / PBO

### 36.5 DSR
採用には DSR gate を通す。  
また、trial count registry を必須とする。

---

## 37. Nautilus Backtest Architecture

### 37.1 role
execution truth。  
signal truth は StateStrike current engine に残し、execution truth を Nautilus に委ねることを許容する。

### 37.2 bridge components
- instruments
- converters
- loaders
- venue config
- core strategy
- reentry strategy
- runner
- compare report

### 37.3 required outputs
- fill ratio
- maker/taker mix
- realized vs simulated cost
- partial fill behavior
- add-on outcome
- soft exit outcome

### 37.4 migration plan
1. Core only  
2. Re-entry  
3. add-on  
4. compare against tiny-live

---

## 38. hftbacktest Calibration

### 38.1 role
queue/latency calibration only

### 38.2 studied orders
- ALO entry
- IOC rescue
- add-on
- soft exit taker fallback

### 38.3 outputs
- optimistic / base / pessimistic fill band
- queue sensitivity
- latency sensitivity
- maker fill probability by symbol and notional bucket

### 38.4 prohibition
hftbacktest の結果だけで strategy 採用可否を決めない。

---

## 39. Security / Secrets / Safety

### 39.1 secrets
- private keys
- API auth
- notification tokens
は env / secret store に置く。repo committed 禁止。

### 39.2 signing
署名の自前実装は避け、tested path を優先。

### 39.3 wallet segregation
- strategy wallet
- reserve wallet
- ops wallet
を分けることを推奨。

### 39.4 safety modes
- global kill switch
- symbol freeze
- lane disable
- live disable requiring explicit `--enable-live`

### 39.5 no-top-up
tiny-live/live でポジション救済のための追加入金禁止。

---

## 40. Deployment

### 40.1 service split
- collector service
- derived batch service
- paper/tiny-live runner
- audit/report service
- alert service

### 40.2 supervision
systemd を標準とする。  
container 必須ではない。

### 40.3 storage
- raw / derived: NVMe Parquet
- state / incidents / intents / fills: SQLite
- optional analytics snapshots: DuckDB

### 40.4 backup
- config
- SQLite
- reports
- selected raw manifests
を定期保存。

---

## 41. Testing Matrix

### 41.1 unit tests
- feature calculation
- state routing
- lane conditions
- order id generation
- reconciliation helpers
- cost audit math

### 41.2 integration tests
- collector reconnect
- raw -> derived
- signal -> intent -> paper fill
- live fallback parser

### 41.3 regression tests
- no lookahead
- no duplicated fills
- no negative qty under add-on
- stale feed stop

### 41.4 replay tests
- historical shock days
- funding boundary hours
- low-liquidity symbols

### 41.5 ops tests
- restart recovery
- schedule cancel refresh
- network disconnect recovery
- DB lock handling

---

## 42. Acceptance Criteria / Definition of Done

### 42.1 System DoD
- all critical services supervised
- restart safe
- reconciliation stable
- audit complete
- secrets externalized

### 42.2 Strategy DoD
- Core + Re-entry live capable
- Fast disabled by default in live
- DSR > threshold
- walk-forward median after-cost > 0
- lane contribution explainable

### 42.3 Execution DoD
- maker-first policy implemented
- rescue bounded
- realized cost audit implemented
- no orphan stop / bracket failures

### 42.4 Ops DoD
- runbooks complete
- incident taxonomy complete
- daily/weekly review artifacts complete

---

## 43. Open Questions

以下は完成品仕様に含めるが、初期既定値は conservative とする。

1. add-on 2 を live default on にするか
2. Re-entry の最大回数
3. value area の profile horizon
4. low-volume node を score に入れるか
5. empirical Kelly cap の係数
6. Fast lane を tiny-live に昇格する条件

これらは research gate を通してから変更する。

---

## 44. 実装優先順位

1. Measurement layer
   - cost surface
   - regime edge table
   - OI/funding quality report
2. Nautilus Core
3. Nautilus Re-entry
4. DSR gate
5. live Core + Re-entry completion
6. add-on full validation
7. Fast research continuation
8. post-liquidation separate playbook

---

## 45. Appendix A: 設定ファイルの概形

```yaml
app:
  mode: tiny_live
  timezone: UTC

venue:
  name: hyperliquid
  network: mainnet
  isolated_only: true
  enable_live: false

universe:
  min_abs_ret_74h: 0.04
  min_volume_ratio_24: 1.15

state:
  profile_horizon_hours: 24
  micro_profile_horizon_hours: 6
  displacement_atr_mult: 0.30
  acceptance_retrace_frac: 0.40

tradability:
  expected_total_cost_bps_max: 16
  impact_bps_max: 6
  min_depth_top3_to_notional: 8.0

funding:
  veto_minutes: 10
  penalty_minutes: 30
  severe_funding_z: 2.0

lanes:
  fast:
    enable_live: false
    risk_fraction: 0.0025
  core:
    risk_fraction: 0.0035
    edge_to_cost_min: 2.2
  reentry:
    risk_fraction: 0.0025
    edge_to_cost_min: 1.8

risk:
  daily_stop_r: -4
  global_stop_r: -6
  cooldown_after_losses: 3

validation:
  dsr_required: true
  walk_forward_windows: 3
```

---

## 46. Appendix B: SQLite tables

### order_intents
```txt
intent_id
ts_created
symbol
lane
side
qty
limit_px
trigger_px
order_style
expected_cost_bps
state_snapshot_json
meta_json
```

### live_fills
```txt
fill_id
ts_fill
symbol
order_id
client_order_id
side
qty
px
fee
liquidity_side
notes_json
```

### live_cost_audit
```txt
audit_id
symbol
client_order_id
expected_total_cost_bps
realized_total_cost_bps
cost_gap_bps
arrival_bps
slippage_bps_vs_intended
fills_count
qty_filled
meta_json
```

### incidents
```txt
incident_id
ts
severity
class
symbol
description
context_json
resolved
```

---

## 47. Appendix C: 主要 feature catalog

### universe
- `ret_74h`
- `volume_ratio_24`

### tradability
- `spread_bps`
- `impact_bps_q`
- `depth_top3_usd`
- `expected_total_cost_bps`

### value / transition
- `POC_prev`
- `VAH_prev`
- `VAL_prev`
- `value_mid_prev`
- `micro_poc_6h`
- `displacement_px`
- `non_reentry_flag`

### flow
- `ofi_30s`
- `ofi_60s`
- `basis_change_30s`
- `impact_recovery_30s`
- `refill_speed_30s`

### perp-state
- `funding_rate`
- `funding_z`
- `minutes_to_next_funding`
- `open_interest`
- `delta_oi_60s`
- `oi_accel_120s`
- `liquidation_shock_flag`

### score / lane
- `core_score`
- `reentry_score`
- `edge_to_cost`
- `expected_mfe_conditional`

---

## 48. Appendix D: 主要 runbooks

### D.1 reconnect
1. ws disconnect 検知
2. reconnect
3. snapshot ack 処理
4. missed range を info query で補完
5. dedup
6. incident 記録

### D.2 stale feed
1. stale threshold 超過
2. 新規注文停止
3. open orders 再照合
4. 必要なら flatten
5. feed 正常化まで freeze

### D.3 missing bracket
1. position open 検知
2. bracket absent 検知
3. 直ちに reduce-only protective order 再送
4. incident 記録
5. symbol freeze

### D.4 schedule cancel failure
1. renewal failure
2. new entry stop
3. alert
4. manual check required

---

## 49. Appendix E: 参考資料の使い方

### E.1 order flow 論文
StateStrike では、OFI と flow persistence の採用根拠に使う。  
ただし、そのまま exact threshold を借りるのではなく、StateStrike の venue/timeframe で再推定する。

### E.2 intraday predictability 論文
liquidity bucket と shock state の分離根拠に使う。  
「常に momentum」でも「常に reversal」でもないことを示すものとして読む。

### E.3 perpetual market quality 論文
funding 近接で spread が悪化しうることから、funding veto と size-down の根拠に使う。

### E.4 OI quality 論文
OI を主シグナルにしない設計根拠に使う。

### E.5 liquidation 論文
post-liquidation を別 state に切る根拠に使う。

### E.6 DSR 論文
採用ゲートそのものの理論根拠に使う。

---

## 50. 最終宣言

本仕様書が定義する完成品は、**「Hyperliquid 上で perpetual continuation を execution-aware に回収するための、Core + Re-entry を中心とした single-venue production system」**である。  
本仕様書は、MVP を超えた完成品の範囲を規定しており、**単なる戦略コード、単なる backtest notebook、単なる paper bot** は完成品とみなさない。

完成品の条件は、次の 3 つに要約できる。

1. **state を正しく切り、入らない場面を明確にすること**
2. **live に近い execution truth を backtest / paper / tiny-live で確認すること**
3. **multiple testing と運用事故の両方に対して、採用ゲートを持つこと**

この 3 つを満たす限り、StateStrike は小資金環境でも「見かけの成績」ではなく「残る可能性のある edge」に集中できる。  
逆に、この 3 つを崩す変更は、いかに見栄えのよい performance を生んでも、本仕様では採用しない。


## 51. モジュール責務一覧

この章は、完成品の実装者が「どの責務をどのモジュールに置くか」で迷わないための章である。  
ここでの目的は、クラス名やファイル名を固定することではない。  
**責務の境界を固定すること**が目的である。

### 51.1 `settings`
責務:
- env / yaml / defaults の統合
- runtime mode ごとの設定切替
- live 禁止フラグの明示
- feature flag の束ね
- version 管理対象の設定スナップショット出力

禁止事項:
- venue 依存ロジック
- signal 計算
- state 計算

### 51.2 `types`
責務:
- canonical dataclass / pydantic model
- strong typing
- symbol / lane / state / order status / incident class の列挙

禁止事項:
- IO
- DB 書き込み
-ロジック

### 51.3 `storage`
責務:
- raw / derived Parquet
- SQLite state
- idempotent upsert
- migration 管理
- snapshot export

禁止事項:
- venue parser ロジック
- signal ロジック

### 51.4 `adapters.hyperliquid`
責務:
- WS/HTTP 接続
- raw message parser
- symbol metadata
- signed live order path
- order / fill / state reconciliation の source access

禁止事項:
- strategy 判定
- risk sizing

### 51.5 `features`
責務:
- raw / canonical から derived feature を作る
- deterministic
- no lookahead
- feature provenance を持つ

禁止事項:
- live order path
- stop 送信

### 51.6 `state`
責務:
- universe
- tradability bucket
- market state
- lane router
- hard/soft gate

禁止事項:
- actual order submission

### 51.7 `signals`
責務:
- Core / Re-entry / Fast の L1/L2/L3 判定
- lane score
- score explanation vector

禁止事項:
- DB write
- live API call

### 51.8 `execution`
責務:
- order style 決定
- cloid 発行
- bracket attach
- rescue policy
- venue route / paper route 抽象化

禁止事項:
- strategy state の再計算

### 51.9 `risk`
責務:
- sizing
- stop distance
- DD stop
- cooldown
- sleeve exposure cap
- no-top-up enforcement

禁止事項:
- market data ingest

### 51.10 `paper`
責務:
- paper positions
- paper fills
- paper incident journal
- paper cost audit

### 51.11 `backtest`
責務:
- current engine backtest
- compare report
- ablation orchestration
- result standardization

### 51.12 `nautilus_bridge`
責務:
- Nautilus instrument config
- data converters
- runner
- report extraction
- strategy parity harness

### 51.13 `calibration`
責務:
- queue model comparison
- latency bands
- maker fill probability calibration
- IOC rescue cost band estimation

### 51.14 `validation`
責務:
- walk-forward orchestration
- DSR
- PBO / multiple testing registry
- acceptance report generation

### 51.15 `ops`
責務:
- health check
- alerting
- runtime mode lock
- runbook invocation
- daily/weekly report scheduler

---

## 52. 主要インターフェース仕様

### 52.1 `CollectorMessage`
最低限次を持つこと。

```python
class CollectorMessage(BaseModel):
    venue: str
    channel: str
    symbol: str | None
    ts_event: int
    ts_ingest: int
    payload: dict
```

`ts_event` は source 側、`ts_ingest` はローカル受信時刻とする。  
後続の derived 計算は原則 `ts_event` 基準で行う。

### 52.2 `CanonicalBookSnapshot`
```python
class CanonicalBookSnapshot(BaseModel):
    venue: str
    symbol: str
    ts_event: int
    bids: list[tuple[float, float]]
    asks: list[tuple[float, float]]
    best_bid: float
    best_ask: float
    mid: float
```

ここでは top-of-book だけでなく少なくとも上位 3 段以上を保持する。  
理由は impact 推定と refill 推定のため。

### 52.3 `SignalEvent`
```python
class SignalEvent(BaseModel):
    venue: str
    symbol: str
    ts_signal: int
    direction: Literal["long", "short"]
    lane: Literal["fast", "core", "reentry"]
    level: Literal["L1", "L2", "L3"]
    score: float
    state: str
    edge_to_cost: float | None
    meta: dict
```

SignalEvent は order intent ではない。  
L1/L2 は reference material であり、L3 のみ execution path に進める。

### 52.4 `OrderIntent`
```python
class OrderIntent(BaseModel):
    intent_id: str
    venue: str
    symbol: str
    side: Literal["buy", "sell"]
    lane: Literal["core", "reentry", "fast"]
    ts_created: int
    order_style: Literal["ALO", "IOC", "GTC"]
    qty: float
    limit_px: float | None
    trigger_px: float | None
    reduce_only: bool
    post_only: bool
    expected_total_cost_bps: float
    state_snapshot: dict
```

### 52.5 `ExecutionDecision`
```python
class ExecutionDecision(BaseModel):
    allow_submit: bool
    reason_code: str
    size_mult: float
    rescue_allowed: bool
    attach_brackets: bool
    cancel_after_ms: int | None
```

### 52.6 `AuditRecord`
```python
class AuditRecord(BaseModel):
    symbol: str
    ts: int
    client_order_id: str
    expected_total_cost_bps: float
    realized_total_cost_bps: float | None
    cost_gap_bps: float | None
    fills_count: int
    notes: dict
```

---

## 53. Value Profile 実装仕様

### 53.1 価格バケット
profile 計算に使う price bucket は、venue の tick size に完全追従するとノイズが多すぎる場合がある。  
そのため完成品では `profile_tick_multiplier` を持つ。既定は 4 とする。

```txt
profile_bucket_size = tick_size * profile_tick_multiplier
```

### 53.2 セッション境界
標準は UTC 日次アンカーとする。  
ただし、将来の比較研究のために次も生成してよい。

- rolling 24h profile
- 8h profile
- 6h micro profile

本番シグナルの primary session は UTC 日次 anchored を使う。

### 53.3 集計値
各バケットについて、少なくとも次を保持する。

- trade notional sum
- trade volume sum
- trade count
- buy/sell aggressor imbalance optional

### 53.4 POC
最も notional volume が大きいバケット。  
複数同値なら、直近 mid に近い方を優先。

### 53.5 Value Area
POC から左右へ volume を足して 70% を覆う帯を構成する。  
細部実装は設定可能だが、production では deterministic であることが必須。

### 53.6 Low/High Volume Nodes
採用は optional とする。  
本番既定値では score に直接入れず、research feature として保持する。  
理由は、外部文献の直接支持が薄く、自前検証が必要なため。

### 53.7 micro-POC
rolling 6h profile から算出する。  
Re-entry での浅い戻り候補点として使う。

---

## 54. State 計算詳細

### 54.1 `AT_UP` 条件
最小構成:

1. `mark_close_15m > VAH_prev + displacement_min`
2. `expected_total_cost_bps` safety floor 内
3. hard veto 非該当
4. prior state が `DISABLED` または反対側の state で固定化されていない

### 54.2 `AT_DN` 条件
Long の反転。

### 54.3 displacement_min の rationale
ATR を使う理由は、銘柄ごとの荒さを吸収するため。  
spread と impact を含める理由は、**意味のない 1 ティック越えを state 化しない**ため。

### 54.4 `BALANCED_VALUE`
`mark_close_15m` が `VAL_prev <= px <= VAH_prev` にあり、かつ `shock_flag == False` のとき。  
この state では continuation 系 lane を一切発火させない。

### 54.5 `POST_LIQUIDATION_DISORDER`
shock 条件を満たした後、所定 bars はこの state に固定する。  
理由は、急変直後は flow 指標や spread 指標が平常時とは意味を変えるから。

### 54.6 state の優先順位
同一 bar で複数候補が立った場合の優先順位は次。

1. `POST_LIQUIDATION_DISORDER`
2. `AT_UP` / `AT_DN`
3. `BALANCED_VALUE`
4. `DISABLED`

---

## 55. Hard Gate / Soft Gate 詳細

### 55.1 hard gate
hard gate は **絶対に新規発注しない条件**である。

対象:
- `minutes_to_next_funding <= 10`
- `liquidation_shock_flag == True`
- stale feed
- DB / collector severe inconsistency
- daily stop active
- strategy drawdown stop active
- global disable active
- venue health degraded
- `M bucket` でない（Core / Re-entry live）
- isolated mode enforced できない
- expected total cost が絶対上限超え

### 55.2 soft gate
soft gate は signal を消すのではなく、score や size を下げる。

対象:
- funding_z やや高い
- delta_oi 弱い
- basis やや悪化
- refill 弱い
- impact recovery 弱い
- edge_to_cost borderline
- time-of-hour unfavorable

### 55.3 実装ルール
hard gate は boolean 明示。  
soft gate は additive penalty または multiplier として実装する。  
両者を混同しないこと。

---

## 56. OFI / Flow feature 詳細

### 56.1 OFI 定義
簡易版では、短期間の aggressor buy notional - aggressor sell notional を使う。  
少なくとも次を持つ。

- `ofi_30s`
- `ofi_60s`

必要に応じて 10s, 120s を研究用に持ってよい。

### 56.2 標準化
`z-score` ないし symbol-aware normalization を行う。  
絶対値比較だけで symbol 間比較してはならない。

### 56.3 persistence
本番 signal の主役は persistence であり、単一 window の強さだけではない。  
たとえば Long なら、
- `ofi_30s_z > 0`
- `ofi_60s_z > 0`
の両方が必要。

### 56.4 flow decay
exit 側では OFI の符号反転または急劣化を soft exit 条件に使う。

### 56.5 注意
OFI 自体は強い候補だが、どの proxy が本当に効くかは venue / timeframe 依存である。  
したがって本番採用 feature は walk-forward の安定性を確認したものだけにする。

---

## 57. OI / Funding 品質監査仕様

### 57.1 目的
OI と funding は重要だが、そのまま信じるのは危険である。  
品質監査を通して、「主シグナルに使ってよい情報」と「confirmation に留める情報」を分ける。

### 57.2 OI 監査項目
- 更新間隔の分布
- 価格急変時の同時性
- volume との整合
- liquidation event 近傍の挙動
- sudden flatline / jump anomalies

### 57.3 funding 監査項目
- next funding time の整合
- funding boundary での spread widening
- magnitude と market quality の関係
- basis / impact との相関

### 57.4 監査結果の使い方
- 品質が高いもの: confirmation or size penalty に使う
- 品質が曖昧なもの: logging はするが signal には入れない
- 品質が悪いもの: abandon

---

## 58. Order Style Policy

### 58.1 Core entry
1. ALO を試みる
2. 所定猶予時間内に未約定で、still-valid なら小さく IOC rescue
3. still-valid でなければ no trade

### 58.2 Re-entry
Re-entry は pullback を待つので、ALO がより自然。  
IOC rescue は Core より慎重に使う。

### 58.3 Fast
paper only では、実験として IOC rescue をやや広めに許す。  
live では disable。

### 58.4 exit orders
- protective stop
- take-profit or reduce-only exit
- soft exit discretionary order generated by system
- urgent flatten

### 58.5 self-trade prevention
同一アドレスの aggressing/resting collision で resting が cancel される venue 特性を考慮し、add-on や rescue は open order 状態を見てから出す。

---

## 59. Cost Audit 数学仕様

### 59.1 expected_total_cost_bps
少なくとも次を含む。

```txt
expected_total_cost_bps
=
fee_entry_bps
+ fee_exit_bps
+ expected_spread_cross_bps
+ expected_impact_entry_bps
+ expected_impact_exit_bps
+ expected_rescue_penalty_bps
```

### 59.2 realized_total_cost_bps
fill 実績から算出する。

### 59.3 cost_gap_bps
```txt
cost_gap_bps = realized_total_cost_bps - expected_total_cost_bps
```

### 59.4 arrival_bps
注文発行時の reference price と実 fill を比較する。  
maker/taker 混在時は weighted average fill で計算する。

### 59.5 lane / symbol / order style 別集計
cost audit は aggregate ではなく、必ず
- lane
- symbol
- order style
- funding distance
- time-of-hour
で切って報告する。

---

## 60. Paper Fill モデル仕様

### 60.1 目的
paper は理想 fill ではなく、現実寄り fill にする。

### 60.2 maker fill
最低限次の 3 モードを持つ。
- optimistic
- base
- pessimistic

### 60.3 rescue fill
IOC rescue は top-book depth と impact から pessimistic に推定する。

### 60.4 paper と live の関係
paper は calibration の対象であり、truth ではない。  
paper の結果は tiny-live で上書きされる。

---

## 61. Tiny-live Guardrails 詳細

### 61.1 per-symbol exposure cap
symbol ごとの notional 上限を持つ。

### 61.2 simultaneous positions cap
同時 open positions 数を制限する。

### 61.3 lane concurrency
同一 symbol・同一方向で
- Core 1
- Re-entry 最大 n
- add-on 最大 m
を制限する。

### 61.4 funding boundary rule
funding 直前 10 分は flatten を優先し、新規は出さない。

### 61.5 heartbeat
collector / execution / reconciliation の heartbeat を持ち、遅延時は新規停止。

---

## 62. 監視・通知仕様

### 62.1 critical alerts
- missing bracket
- duplicate position
- stale feed
- schedule cancel failure
- live cost gap severe
- DB corruption / locked severe
- symbol freeze triggered

### 62.2 warning alerts
- unusually high realized cost
- OFI feed missing
- funding metadata mismatch
- OI quality degraded
- unusual spread regime

### 62.3 digest
- daily digest
- weekly review digest

---

## 63. Incident Response Policy

### 63.1 Severity 1
資金保全に直結。  
例:
- stop missing
- duplicate unintended position
- wallet/auth anomaly

対応:
- global safe stop
- flatten if necessary
- incident open mandatory

### 63.2 Severity 2
戦略品質重大毀損。  
例:
- severe stale feed
- broken reconciliation
- cost audit severe deviation

対応:
- symbol or lane freeze
- no new entries
- root cause analysis

### 63.3 Severity 3
品質劣化だが即時資金保全には直結しない。  
例:
- report missing
- moderate schema drift
- delayed digest

---

## 64. Walk-forward / Dataset Split 仕様

### 64.1 原則
time series の順序を守る。  
ランダム split 禁止。

### 64.2 standard split
- train
- validation
- forward test
を順送りで複数窓作る。

### 64.3 acceptance
採用には少なくとも
- 3 forward windows
- median after-cost expectancy > 0
- DSR pass
が必要。

### 64.4 lane 別評価
Core と Re-entry を別々に評価し、合算評価だけで採用してはならない。

---

## 65. Multiple Testing Registry

### 65.1 何を記録するか
- 試した feature family
- 試した threshold family
- 試した lane variants
- data period
- selection criterion
- selected model rank

### 65.2 なぜ必要か
「良く見えるものを採った」回数を後から消せないようにするため。

### 65.3 運用ルール
trial registry を更新しない実験結果は採用議論に入れてはならない。

---

## 66. Configuration Governance

### 66.1 versioning
設定変更は versioned artifact として残す。  
live 実行時には設定ハッシュを audit log に必ず残す。

### 66.2 categories
- static config
- operational config
- risk config
- research config

### 66.3 変更の承認
- research only: 単独変更可
- paper/tiny-live: review required
- live: change ticket required

---

## 67. Performance Requirements

### 67.1 latency target
本システムは HFT ではない。  
したがって、絶対最短のレイテンシを競争しない。  
ただし、次は守る。

- signal generation lag: 許容内
- order submit lag: lane policy が機能する範囲
- reconciliation lag: orphan を発生させない範囲

### 67.2 throughput
collector は multi-symbol で安定稼働し、raw loss を起こさないこと。

### 67.3 memory
長時間稼働で memory leak しないこと。

---

## 68. Security Requirements

### 68.1 least privilege
live wallet は必要最小限の資金のみ保持。

### 68.2 code path separation
research mode と live mode の code path は feature flag で明示的に分ける。  
`enable_live` なしに live order path へ入ってはならない。

### 68.3 auditability
すべての live intent は理由・設定・state snapshot と共に残ること。

---

## 69. Migration / Compatibility

### 69.1 current engine coexistence
完成品への移行中は、current backtest engine と Nautilus backtest を併存させる。  
即時置換しない。

### 69.2 live compatibility
order ID, audit schema, fills schema の互換性を保つ migration script を用意する。

### 69.3 rollback
新バージョンが問題を起こした場合、前バージョンの config + DB schema へ戻せること。

---

## 70. Final Product Readiness Checklist

### 70.1 Research readiness
- raw complete
- derived reproducible
- trial registry working
- DSR implemented

### 70.2 Paper readiness
- paper fills
- paper positions
- paper add-on
- paper soft exit
- paper cost audit

### 70.3 Tiny-live readiness
- signed orders
- user stream reconciliation
- schedule cancel
- realized cost audit
- incident alerting

### 70.4 Production readiness
- 21d paper stable
- 14d tiny-live stable
- after-cost positive in forward tests
- severe incidents zero
- Core + Re-entry only
- Fast disabled in live

---

## 71. 参考実装上の疑似コード

### 71.1 Core Long
```python
if not universe_ok(symbol):
    return

if hard_veto_active(symbol, ts):
    return

state = compute_state_15m(symbol, ts)
if state != "AT_UP":
    return

lane_ctx = compute_lane_context_5m(symbol, ts)
if not lane_ctx.core_candidate:
    return

flow30 = compute_flow_30s(symbol, ts)
flow60 = compute_flow_60s(symbol, ts)
if not (flow30.ofi_z > 0 and flow60.ofi_z > 0):
    return

if flow60.delta_oi_z < 0:
    return

tradability = compute_tradability(symbol, ts, notional)
if tradability.bucket != "M":
    return

edge_cost = compute_edge_to_cost(symbol, ts, lane="core")
if edge_cost < cfg.core.edge_to_cost_min:
    return

decision = execution_policy_core_long(...)
if decision.allow_submit:
    submit_order(...)
```

### 71.2 Re-entry Long
```python
if not prior_core_up_state(symbol):
    return

if hard_veto_active(symbol, ts):
    return

reentry_ctx = compute_reentry_context_5m(symbol, ts)
if not reentry_ctx.shallow_pullback_ok:
    return

flow30 = compute_flow_30s(symbol, ts)
flow60 = compute_flow_60s(symbol, ts)
if not flow30.reacceleration_ok:
    return

tradability = compute_tradability(symbol, ts, notional)
if tradability.bucket != "M":
    return

edge_cost = compute_edge_to_cost(symbol, ts, lane="reentry")
if edge_cost < cfg.reentry.edge_to_cost_min:
    return

submit_order(...)
```

---

## 72. 最後に

完成品仕様書として最も重要なのは、  
**「どれだけ多くのことをするか」ではなく、「どの境界を絶対に越えないか」を固定すること**である。

StateStrike の完成品は、次の 5 つを守る限り、実装が進んでも思想が壊れない。

1. Hyperliquid only
2. isolated only
3. Core + Re-entry live only
4. execution-aware validation required
5. DSR / walk-forward / incident gate required

この 5 つを守る限り、value migration の定義や score の細部は改善してよい。  
逆に、この 5 つを崩す変更は、いかに backtest が綺麗でも完成品の仕様から逸脱する。


## 73. 設定項目完全一覧

この章は、完成品で管理対象とする設定項目を網羅的に列挙する。  
実装者は、ここにない設定を無制限に増やしてはならない。  
新しい設定を追加する場合は、どの family に属するかを明確にし、trial registry と変更管理に組み込む。

### 73.1 app family
- `app.name`
- `app.mode`
- `app.env`
- `app.timezone`
- `app.instance_id`
- `app.run_id`

### 73.2 venue family
- `venue.name`
- `venue.network`
- `venue.enable_live`
- `venue.isolated_only`
- `venue.default_order_notional_usd`
- `venue.allowed_symbols`
- `venue.denied_symbols`

### 73.3 data family
- `data.raw_root`
- `data.derived_root`
- `data.sqlite_path`
- `data.partitioning`
- `data.retention_days_raw`
- `data.retention_days_derived`
- `data.profile_tick_multiplier`

### 73.4 collector family
- `collector.ws_ping_interval`
- `collector.reconnect_backoff_min_ms`
- `collector.reconnect_backoff_max_ms`
- `collector.flush_interval_ms`
- `collector.max_in_memory_events`
- `collector.snapshot_recovery_enabled`

### 73.5 universe family
- `universe.min_abs_ret_74h`
- `universe.min_volume_ratio_24`
- `universe.freeze_symbol_hours_after_delist_signal`
- `universe.exclude_isolated_only_if_needed`（通常 false）

### 73.6 profile family
- `profile.session_hours`
- `profile.micro_session_hours`
- `profile.value_area_pct`
- `profile.price_bucket_tick_mult`

### 73.7 state family
- `state.displacement_atr_mult`
- `state.displacement_spread_mult`
- `state.displacement_impact_mult`
- `state.acceptance_retrace_frac`
- `state.post_liquidation_lock_bars`
- `state.min_bars_for_acceptance`
- `state.max_bars_for_acceptance`

### 73.8 tradability family
- `tradability.expected_total_cost_bps_abs_max`
- `tradability.impact_bps_abs_max`
- `tradability.min_depth_ratio`
- `tradability.edge_to_cost_core`
- `tradability.edge_to_cost_reentry`
- `tradability.edge_to_cost_fast`

### 73.9 funding family
- `funding.veto_minutes`
- `funding.penalty_minutes`
- `funding.severe_funding_z`
- `funding.penalty_funding_z`
- `funding.addon_disable_z`

### 73.10 shock family
- `shock.ret_1m_atr_mult`
- `shock.delta_oi_z_abs_min`
- `shock.spread_shock_z_min`
- `shock.lock_bars`

### 73.11 lane family
- `fast.enable_live`
- `fast.risk_fraction`
- `fast.score_min`
- `fast.max_hold_bars`
- `fast.rescue_allowed`
- `core.risk_fraction`
- `core.score_min`
- `core.max_hold_bars`
- `core.rescue_allowed`
- `reentry.risk_fraction`
- `reentry.score_min`
- `reentry.max_hold_bars`
- `reentry.max_entries_per_state`

### 73.12 sizing family
- `sizing.use_empirical_kelly_cap`
- `sizing.kelly_cap_fraction`
- `sizing.max_symbol_exposure_frac`
- `sizing.max_total_open_risk_frac`
- `sizing.min_notional_usd`

### 73.13 stop family
- `stop.atr_mult_core`
- `stop.atr_mult_reentry`
- `stop.atr_mult_fast`
- `stop.min_stop_bps`
- `stop.use_acceptance_level_override`

### 73.14 add-on family
- `addon.enable_live`
- `addon.enable_stage_1`
- `addon.enable_stage_2`
- `addon.stage1_r_threshold`
- `addon.stage2_r_threshold`
- `addon.stage1_size_frac`
- `addon.stage2_size_frac`
- `addon.disable_near_funding_minutes`

### 73.15 exit family
- `exit.soft_ofi_decay_z`
- `exit.soft_basis_adverse_z`
- `exit.soft_edge_to_cost_min`
- `exit.soft_no_new_extreme_bars`
- `exit.force_flatten_before_funding_minutes`

### 73.16 risk family
- `risk.daily_stop_r`
- `risk.global_stop_r`
- `risk.strategy_dd_stop_r`
- `risk.cooldown_after_losses`
- `risk.cooldown_minutes`
- `risk.no_topup`

### 73.17 validation family
- `validation.walkforward_windows`
- `validation.dsr_required`
- `validation.dsr_min`
- `validation.min_closed_trades_for_live_eval`
- `validation.paper_days_required`
- `validation.tiny_live_days_required`

### 73.18 ops family
- `ops.alert_webhook`
- `ops.email_enabled`
- `ops.daily_report_time_utc`
- `ops.weekly_review_day_utc`
- `ops.schedule_cancel_refresh_seconds`

---

## 74. データ品質ルール

### 74.1 欠損
欠損を黙って無視してはならない。  
次のどれかに分類する。

- recoverable gap
- non-recoverable gap
- stale but contiguous
- source outage

### 74.2 recoverable gap
snapshot ack や info query で補完可能。  
補完後に derived 再生成を許可。

### 74.3 non-recoverable gap
補完不能。  
その時間帯は backtest / paper / live の signal 生成対象から除外し、監査ログへ記録する。

### 74.4 clock skew
`ts_event` と `ts_ingest` の差が異常なときは skew anomaly として別管理する。  
skew anomaly は cost audit や flow calculation の信頼性を落とすため、severe なら hard veto の一部とする。

### 74.5 duplicate
raw dedup は collector または canonical layer で実施する。  
重複イベントを signal layer に流してはならない。

### 74.6 schema drift
外部 API が変わった場合、parse success だけで安全とみなしてはならない。  
field absence / semantic change / unit change を検出し、incident を上げる。

---

## 75. Metric 定義辞書

### 75.1 pnl 系
- `gross_pnl_usd`: 手数料・スリッページ控除前
- `net_pnl_usd`: 控除後
- `net_pnl_r`: initial risk を基準にした R multiple

### 75.2 risk 系
- `current_drawdown_r`
- `daily_drawdown_r`
- `open_risk_usd`
- `open_risk_frac`

### 75.3 cost 系
- `expected_total_cost_bps`
- `realized_total_cost_bps`
- `cost_gap_bps`
- `arrival_bps`
- `slippage_bps_vs_intended`

### 75.4 fill 系
- `maker_fill_ratio`
- `fills_count`
- `partial_fill_ratio`
- `rescue_usage_ratio`

### 75.5 edge 系
- `edge_to_cost`
- `conditional_mfe_bps`
- `conditional_mae_bps`
- `soft_exit_saved_loss_bps`

### 75.6 signal 系
- `opportunity_count`
- `tradable_count`
- `executable_count`
- `submit_count`
- `fill_count`
- `lane_count_fast`
- `lane_count_core`
- `lane_count_reentry`

### 75.7 quality 系
- `incident_count_by_class`
- `stale_feed_minutes`
- `gap_count`
- `oi_quality_score`
- `funding_quality_score`

---

## 76. 週次レビュー・テンプレート

週次レビューは任意ではない。  
完成品では週次レビューが運用の一部である。

### 76.1 必須レビュー項目
1. 実現損益
2. lane 別損益
3. symbol 別損益
4. funding distance bucket 別損益
5. cost gap 分布
6. missed entry / orphan / incident
7. signal 数と submit 数の乖離
8. tiny-live と backtest の差
9. config change の有無
10. 翌週 freeze すべき symbol / lane の有無

### 76.2 禁止事項
- PnL だけを見て継続/停止を決めること
- trial registry を見ずに feature を追加すること
- incident を「小さいから」と握りつぶすこと

### 76.3 レビュー出力
- Markdown レポート
- JSON metrics dump
- CSV summary
- optional chart pack

---

## 77. シンボル管理ポリシー

### 77.1 allowlist / denylist
システムは allowlist と denylist の両方をサポートする。  
本番では基本的に allowlist 運用を推奨する。

### 77.2 symbol freeze
次の場合、symbol freeze を許可する。
- repeated severe incidents
- consistently abnormal cost gap
- suspicious OI/funding behavior
- extreme spread regime persistence
- oracle risk suspicion

### 77.3 relisting
freeze 解除には review が必要。  
自動解除してはならない。

---

## 78. 手数料・最小取引額・venue 例外処理

### 78.1 手数料
perps fee の current tier を明示的に config または metadata として取得する。  
hard-code してよいが、監査で検知できるようにする。

### 78.2 minimum notional
venue の min trade notional を満たさない size は発注しない。  
size rounding 後に閾値を下回るケースに注意する。

### 78.3 tick / lot rounding
注文前に必ず tick size / lot size へ丸める。  
rounding により risk が過大化する場合は減方向へ丸める。

### 78.4 cap 例外
OI cap, oracle distance, leverage tier, max market order value など venue 制約を pre-check する。  
reject を strategy layer で待つのではなく、execution layer で先に弾く。

---

## 79. Research-to-Production 昇格ルール

### 79.1 feature 昇格
research feature を production score に入れるには次を満たす。
- walk-forward で安定
- DSR pass
- tiny-live で directionally consistent
- code complexity に見合う improvement

### 79.2 lane 昇格
Fast のような研究 lane を live へ昇格するには次を満たす。
- paper 21日で quality acceptable
- tiny-live A/B で cost gap manageable
- Core / Re-entry の quality を毀損しない

### 79.3 playbook 昇格
post-liquidation recovery のような別戦略を live へ入れるには、Core + Re-entry と別に独立 validation を要求する。  
本線の validation を流用してはならない。

---

## 80. Human Override Policy

### 80.1 override は原則例外
完成品は自律運用を前提とする。  
人手 override は例外であり、ログ必須。

### 80.2 許可する override
- emergency global stop
- symbol freeze
- live disable
- flatten all
- known incident workaround

### 80.3 許可しない override
- discretionary entry
- stop widen
- no-topup 破り
- DD stop 無視

### 80.4 記録
override には
- who
- when
- why
- before state
- after state
を残す。

---

## 81. 実運用の平常時サイクル

### 81.1 日中常時
- collectors 稼働
- derived batch 更新
- paper/live runner 稼働
- schedule cancel 更新
- user stream reconciliation 継続
- alerts watch

### 81.2 日次
- cost audit report
- PnL report
- incident summary
- symbol health summary
- OI/funding quality snapshot

### 81.3 週次
- strategy review
- config review
- trial registry review
- lane contribution review
- freeze/allowlist review

### 81.4 月次
- archive and backup
- schema migration check
- dependency update review
- security review

---

## 82. 性能と資源計画

### 82.1 CPU
主な負荷源:
- collector parse
- profile calc
- derived batch
- Nautilus replay
- report generation

### 82.2 RAM
大量 raw replay や multiple symbols の L2 保持が重い。  
したがって、offline backtest と online service の同居時は resource partitioning を行う。

### 82.3 Disk
raw book/trade 保存量が大きい。  
retention と compression を設計に含める。

### 82.4 IO contention
derived batch と raw ingest が同じディスクで競合しないよう配慮する。  
NVMe 前提でも夜間 batch 推奨。

---

## 83. バッチ設計

### 83.1 incrementality
full rebuild だけでなく incremental rebuild をサポートする。  
ただし、profile や state が参照する horizon をまたぐときは完全再計算が必要になり得る。

### 83.2 open bar exclusion
未確定 bar を signal 判定に使わない。  
backtest / paper / tiny-live で同じ規則を適用する。

### 83.3 materialization order
1. raw validation
2. canonical
3. bars
4. profiles
5. cost surface inputs
6. features
7. states
8. signals
9. backtest/report

---

## 84. Current Engine と Nautilus の役割分担

### 84.1 Current Engine
- feature truth
- state truth
- lane truth
- ops truth
- audit truth

### 84.2 Nautilus
- execution truth
- order book fill truth
- backtest fill realism

### 84.3 比較原則
strategy logic はできるだけ同じにし、違いは execution layer に押し込む。  
そうでないと、何が performance 差の原因か分からなくなる。

---

## 85. compare report 仕様

compare report は current engine と Nautilus、および paper/tiny-live を並べる。  
最低限次を含む。

- trade count
- executable count
- fill count
- maker fill ratio
- avg realized cost
- avg cost gap
- net expectancy
- MFE/MAE
- lane contribution
- symbol bucket contribution
- funding distance bucket contribution

比較は aggregate だけでなく conditional に行う。

---

## 86. Abalation Study 規則

### 86.1 目的
feature を増やした結果、何が効いたのか分解する。

### 86.2 必須比較
- Core only
- Core + Re-entry
- Core + Re-entry + add-on1
- Core + Re-entry + add-on1/2
- funding veto off/on
- OI confirmation off/on
- profile-based boundary vs recent-high/low boundary

### 86.3 出力
- absolute metrics
- delta metrics
- DSR delta
- trial count increment

### 86.4 禁止
複数変更を同時に入れて「良くなった」と言うこと。

---

## 87. 受入テスト詳細

### 87.1 Data acceptance test
- 24h raw continuity
- no fatal schema drift
- no unresolved severe gaps

### 87.2 Strategy acceptance test
- lane counts within expected range
- Core / Re-entry sign symmetry sanity
- hard veto works
- score distributions sane

### 87.3 Execution acceptance test
- cloid deterministic
- bracket attach success
- cancel/replace success
- no orphan position

### 87.4 Audit acceptance test
- every fill has intent
- every intent has outcome or explicit cancel
- cost audit complete ratio >= target

### 87.5 Ops acceptance test
- service restart safe
- snapshot recovery works
- alert path works

---

## 88. 期待値の定義と判定

### 88.1 期待値
完成品では、期待値を次のどれか1つで語ってはならない。
- win rate だけ
- ROI だけ
- Sharpe だけ

### 88.2 最低限見るべきもの
- net expectancy after cost
- DSR
- max drawdown
- cost gap
- lane contribution
- state-conditioned expectancy

### 88.3 低資金向けの見方
元本が小さいため、回数不足は問題である。  
ただし、それを final executable count の乱造で解決してはならない。  
解決策は L1/L2/L3 分離と Re-entry 増である。

---

## 89. 実装時のアンチパターン

1. state と signal を同一関数で書く
2. venue parser の中で strategy 判定をする
3. raw を保存せず derived だけ持つ
4. OI を主シグナル化する
5. funding 近接を無視する
6. Fast lane を live default on にする
7. DSR なしで feature を増やす
8. current backtest だけで採用を決める
9. orphan bracket を許す
10. live で no-topup を破る

---

## 90. 最終補足

本仕様書は「実装が簡単かどうか」を基準に書いていない。  
**完成品として必要かどうか**を基準に書いている。

したがって、
- 現時点で未実装の項目
- research の裏付けが十分でない項目
- performance 上まだ厳しい項目
も含まれている。

しかし、それらを仕様から外すと、できるのは MVP であって完成品ではない。  
この文書はその線引きを意図している。

運用者は、この文書を「今すぐ全部実装しろ」という意味で読む必要はない。  
ただし、**完成品と呼ぶためにどこまで必要か**の基準として使わなければならない。


## 91. Core lane ルール表（詳細）

以下は Core lane のルールを、人間がレビューしやすいように条件表にしたものである。  
実装では同等ロジックを満たせばよいが、意味を変えてはならない。

### 91.1 Core Long 条件表

| 層 | 条件 | 種別 | 説明 |
|---|---|---|---|
| Universe | `abs(ret_74h) >= 4%` | 固定 | 眠っている銘柄を除外 |
| Universe | `volume_ratio_24 >= 1.15` | 固定 | 参加者が増えていること |
| State | `mark_close_15m > VAH_prev + displacement_min` | 仕様決定 | 旧価値帯の上へ意味ある変位 |
| State | `shock_flag == False` | hard | liquidation disorder でない |
| Funding | `minutes_to_next_funding > 10` | hard | funding 直前禁止 |
| Acceptance | `non_reentry_up == True` | 仕様決定 | 旧価値帯へ深く戻らない |
| Flow | `ofi_30s_z > 0` | soft/core必須 | 主方向の押し |
| Flow | `ofi_60s_z > 0` | soft/core必須 | 継続確認 |
| Confirmation | `delta_oi_60s_z >= 0` | soft | 新規参加が逆行していない |
| Tradability | `bucket == M` | hard | continuation は M だけ |
| Cost | `edge_to_cost >= core_min` | hard | 期待移動がコストを十分上回る |
| Risk | `global guards pass` | hard | DD stop 等非発火 |
| Execution | `order_style decidable` | hard | ALO/IOC/no-trade が決められる |

### 91.2 Core Short 条件表
上表の符号を反転したもの。  
`VAH_prev` は `VAL_prev` に、`up` は `down` に読み替える。

### 91.3 Core lane の発注順
1. L1 記録  
2. L2 記録  
3. L3 判定  
4. intent 生成  
5. ALO 送信  
6. 未約定で still-valid の場合のみ rescue 判定  
7. bracket attach  
8. fill reconciliation  
9. cost audit

### 91.4 Core lane が不成立になる典型例
- value area を越えたが displacement が不足
- 1 bar だけ外に出たが 2 bar 目で value area に戻る
- OFI が正でない
- expected cost が高い
- funding 直前
- shock state

---

## 92. Re-entry lane ルール表（詳細）

### 92.1 Re-entry Long 条件表

| 層 | 条件 | 種別 | 説明 |
|---|---|---|---|
| Precondition | prior `AT_UP` or live core long exists | hard | 上方向 state が生きていること |
| Pullback | shallow pullback to `VAH_prev` / `micro_poc` / `VWAP` | 仕様決定 | 浅い戻りであること |
| Pullback | no deep reentry to old value area | hard | 旧価値帯へ戻りすぎない |
| Flow | `ofi_30s_z > 0` or re-acceleration positive | hard | 買い再加速 |
| Persistence | `ofi_60s_z` not strongly negative | soft/hard threshold | 1分で崩れていない |
| Tradability | `bucket == M` | hard | M以外禁止 |
| Cost | `edge_to_cost >= reentry_min` | hard | コスト見合い |
| Funding | `minutes_to_next_funding > 10` | hard | 直前禁止 |
| Risk | additional same-side exposure cap not exceeded | hard | 同方向の増やし過ぎ防止 |

### 92.2 Re-entry Short 条件表
Long の逆。

### 92.3 Re-entry の意味
Re-entry は loser averaging ではない。  
**trend state が継続している前提のもとで、浅い押し/戻りの再加速を取る別シグナル**である。  
したがって、
- entry price が initial core より有利でも
- old value area 深部に戻っていれば  
不成立である。

### 92.4 Re-entry の最大回数
production default:
- 1 state あたり最大 2 回
- tiny-live では 1 回から始め、validation 後に増やす

---

## 93. Fast lane ルール表（詳細）

### 93.1 Fast の位置づけ
Fast は signal count を増やすが、期待値と再現性の観点では本線ではない。  
したがって、仕様に含むが、live default off とする。

### 93.2 Fast Long 条件例
- `AT_UP` 候補が出ている
- 5m displacement あり
- 30s OFI 正
- severe funding/shock なし
- expected cost 絶対上限内
- edge_to_cost は Core より低い基準で許可

### 93.3 Fast Short 条件例
逆。

### 93.4 Fast の終了条件
- time stop 短め
- non-reentry 崩れ
- OFI decay
- cost deterioration

### 93.5 Fast を採用する条件
- paper 21日で acceptable
- tiny-live micro sleeve で cost gap manageable
- Core / Re-entry の quality を壊さない

---

## 94. execution route 決定仕様

### 94.1 route 候補
- `ALO_ONLY`
- `ALO_THEN_IOC_RESCUE`
- `NO_TRADE`
- `EMERGENCY_FLATTEN`

### 94.2 route 決定入力
- lane
- edge_to_cost
- top-book depth
- maker fill proxy
- funding distance
- shock state
- current open orders
- same-side exposure

### 94.3 ALO_ONLY 条件
- maker fill proxy 高
- edge_to_cost 十分
- time sensitivity 低
- reentry 系

### 94.4 ALO_THEN_IOC_RESCUE 条件
- Core lane
- state がまだ有効
- missed fill の期待損失が rescue cost を上回る
- IOC size 上限内

### 94.5 NO_TRADE 条件
- still-valid 条件崩れ
- rescue すると cost が edge を食う
- open orders / position state が複雑すぎる

### 94.6 EMERGENCY_FLATTEN 条件
- orphan / missing stop
- severe stale feed
- severe reconciliation failure

---

## 95. Bracket 管理仕様

### 95.1 基本
すべての本番 position には保護 bracket が必要。  
「後で付ける」は許容するが、「付かないまま保持」は許容しない。

### 95.2 bracket の種類
- protective stop
- optional take-profit
- add-on aware stop resize

### 95.3 attach タイミング
- fill 直後
- partial fill でも保護 stop を先に付ける
- multiple fills の平均建値が確定したら更新してよい

### 95.4 missing bracket
critical incident。  
symbol freeze し、protective reduce-only order を最優先で再送する。

### 95.5 add-on 時の再計算
add-on 後は
- avg entry
- total qty
- risk amount
- bracket qty
を再計算する。  
これを paper/backtest/live で一致させる。

---

## 96. schedule cancel / dead-man’s switch 仕様

### 96.1 目的
ネットワーク断やプロセス障害時に、resting orders が無人で残ることを防ぐ。

### 96.2 ポリシー
- live mode では定期更新必須
- 更新失敗で新規停止
- 連続失敗で global live disable

### 96.3 運用
- collector と execution heartbeat の双方が正常であるときのみ refresh
- refresh 成功ログ必須
- 期限切れまでの残り秒数をメトリクス化

### 96.4 監査
週次レビューで
- refresh failure count
- grace period remaining anomalies
を確認する。

---

## 97. PnL 会計仕様

### 97.1 realized / unrealized
realized と unrealized を混ぜない。  
risk guard や DD 計算にどちらを使うかを明示する。

### 97.2 fee accounting
maker rebate / fee discount など venue 特性を吸収できるよう、fee は absolute と bps の両方で保存する。

### 97.3 USDC/USDT quanto 的性質
Hyperliquid の contract specs 上、USDC collateral / USDT-denominated linear の quanto 的要素がある。  
Pnl 表示と risk 評価では venue 表現に合わせつつ、会計保存では base/quote/collateral を混同しない。

### 97.4 lane attribution
PnL は必ず lane attribution を持つ。  
Core / Re-entry / add-on を同一 trade bucket に潰してはならない。

---

## 98. Symbol Selection 拡張仕様

### 98.1 universe 通過後の並び替え
シグナルは全通過させるが、execution 優先度は次で並べる。
- edge_to_cost 高
- M bucket quality 高
- incident history 低
- funding distance 大
- cost gap 安定

### 98.2 simultaneous candidates
同時に複数シンボルが executable になった場合、優先順位決定器を用意する。  
単純な arrival order にしない。

### 98.3 selection freeze
選択結果は intent 時点で確定し、後続の市場変化で優先順位を再計算しても、発注直前以外ではひっくり返さない。

---

## 99. 低流動性銘柄方針

### 99.1 基本
低流動性銘柄は universe に入っても、continuation の本線とは限らない。  
L bucket は研究用に残すが、本番 continuation は M bucket のみ。

### 99.2 理由
- momentum/reversal の性質が変わりやすい
- spread/impact が jump 的に悪化しやすい
- maker fill の楽観化が起きやすい
- liquidation shock と見分けにくい

### 99.3 研究用の扱い
L bucket の有望 state は research に残し、将来別 playbook として切り出す。

---

## 100. 監査レポートの章立て

完成品の自動レポートは少なくとも次の章を持つ。

1. エグゼクティブサマリー
2. 総損益
3. lane 別損益
4. symbol 別損益
5. cost audit
6. fill quality
7. funding distance 分布
8. state 分布
9. incidents
10. config hash / code version
11. 前週比
12. 要対応事項

---

## 101. Strategy Review Meeting 議事テンプレート

### 101.1 議題
- 先週の損益より、先週の「何が効いて何が壊れたか」を議題にする
- trial registry 更新確認
- 新 feature 提案の事前審査
- symbol freeze / unfreeze
- config 変更候補
- Fast の昇格可否
- add-on stage2 の有効化可否

### 101.2 参加者
最小:
- オーナー
- 実装担当
- 運用担当（兼任可）

### 101.3 決定事項の記録
- 変更内容
- 根拠
- 適用日
- rollback 条件

---

## 102. Code Review 基準

### 102.1 必須観点
- no lookahead
- deterministic behavior
- venue leak の有無
- logging 十分性
- audit trail
- tests の更新

### 102.2 差し戻し条件
- strategy と adapter の責務混在
- hard/soft gate 混同
- raw 未保存
- unbounded config addition
- lane attribution 消失

### 102.3 Live path 追加レビュー
live path の変更は通常 review より厳格にし、
- signing
- order IDs
- reconciliation
- risk guard
- schedule cancel
を重点監査する。

---

## 103. Release Policy

### 103.1 versioning
- `major`: アーキテクチャや acceptance の意味変更
- `minor`: lane / feature / audit 拡張
- `patch`: bug fix

### 103.2 release channels
- `research`
- `paper`
- `tiny-live`
- `live-stable`

### 103.3 promoting rule
research -> paper -> tiny-live -> live の順にしか昇格できない。  
飛び級禁止。

### 103.4 rollback trigger
- severe incidents
- cost gap blowout
- unexplained signal drift
- reconciliation mismatch

---

## 104. Documentation Policy

### 104.1 文書群
完成品は、少なくとも次の文書を持つ。
- 本仕様書
- 運用 runbook
- incident taxonomy
- config reference
- API / schema reference
- validation protocol
- release notes

### 104.2 更新規則
コードだけ更新して文書を放置してはならない。  
特に live path の変更は文書更新必須。

### 104.3 docs as source of truth
「コードを見れば分かる」は不可。  
完成品では文書が一次の運用基準である。

---

## 105. 終章: この仕様書で何が固定されたか

この仕様書で固定されたことは次の通り。

1. **完成品の定義**
2. **Hyperliquid / isolated / Core + Re-entry 本番**
3. **auction transition を主軸にする state machine**
4. **L1/L2/L3 シグナル階層**
5. **Nautilus を execution truth に使う方針**
6. **hftbacktest を calibration 専用に使う方針**
7. **OI は confirmation に留めること**
8. **funding 近接は veto / size-down の対象であること**
9. **DSR と walk-forward を採用ゲートに入れること**
10. **paper/tiny-live/live を完成品範囲に含めること**

この 10 項目が固定されたことで、StateStrike は「よく見える戦略案」ではなく、**完成品を作るための設計対象**になった。  
以後の議論は、この固定点を前提に行う。


## 106. 具体例シナリオ集

本章は、抽象仕様を実装・レビューしやすくするための具体例である。  
数値は説明用の例であり、exact threshold ではない。  
ただし、**何を成功・失敗とみなすかの意味**は本仕様と一致しなければならない。

### 106.1 Core Long 成功例
前提:
- Universe 通過
- M bucket
- funding まで 42 分
- shock flag なし

観測:
- 前日 `VAH_prev = 100.0`
- `ATR_15m = 3.0`
- `spread_px_5m = 0.10`
- `impact_px = 0.40`
- `mark_close_15m = 101.4`

ここで
- `0.30 * ATR = 0.9`
- `2 * spread = 0.2`
- `0.5 * impact = 0.2`
なので displacement_min = 0.9。  
実 displacement = 1.4。  
よって `AT_UP` 候補成立。

次に 5m で、
- 2 本連続 close が 100.0 より上
- deepest retrace が 0.35  
なので non-reentry 成立。

次に
- `ofi_30s_z = 1.3`
- `ofi_60s_z = 0.8`
- `delta_oi_60s_z = 0.4`
- `edge_to_cost = 2.6`

これにより Core Long executable。  
注文は ALO 優先、still-valid 時のみ IOC rescue。

### 106.2 Core Long 失敗例
同じく `AT_UP` 候補は出たが、
- 5m の 2 本目で close が `VAH_prev` の内側に戻る
- `ofi_60s_z` も負へ反転  
この場合、L1 は記録されても L3 には進まない。  
ここで無理に買うのは本仕様違反。

### 106.3 Re-entry Long 成功例
Core Long 後に価格がやや押し、
- `micro_poc_6h`
- `session_vwap`
の近傍まで戻る。  
しかし `VAH_prev` 深部には戻らない。  
そこで
- `ofi_30s_z` が再び正へ加速
- `ofi_60s_z` も極端に悪くない
- cost も許容内  
なら Re-entry Long L3 へ進める。

### 106.4 funding 近接で見送る例
条件がすべて良くても `minutes_to_next_funding = 8` なら hard veto。  
「もったいないから入る」は許されない。

### 106.5 shock 後で見送る例
1 分で大きな急変、spread_shock, delta_oi 急変が出ているなら `POST_LIQUIDATION_DISORDER`。  
この状態では continuation lane を出さない。  
Post-shock recovery を取りたければ、別 playbook の validation が必要。

---

## 107. 実装時の状態遷移図（文章版）

### 107.1 通常フロー
1. raw 収集
2. canonical 化
3. derived features 更新
4. 15m state 計算
5. 5m lane 候補計算
6. 30s/60s flow persistence 計算
7. L1/L2/L3 signal 計算
8. execution decision
9. intent 保存
10. 注文送信
11. user stream / polling で fill / state 追跡
12. bracket attach
13. cost audit
14. position 管理
15. exit
16. report

### 107.2 エラー時フロー
1. raw 欠損検知
2. recoverable か判定
3. recoverable なら snapshot/info 補完
4. non-recoverable なら signal 停止、incident
5. severe なら global safe stop

### 107.3 live での orphan 検知フロー
1. clearinghouseState に position あり
2. local state に position なし  
-> orphan position incident  
3. openOrders 照合  
4. protective reduce-only order の有無確認  
5. 無ければ直ちに protective order 発注  
6. symbol freeze

---

## 108. 実装タスクの依存関係

### 108.1 先に必要なもの
- raw collector 安定化
- canonical schema 固定
- cost audit 基盤
- OI/funding quality report skeleton
- config governance

### 108.2 その後でないと危険なもの
- Fast lane live 化
- add-on stage2 本番化
- post-liq recovery 本番化
- ML veto/size

### 108.3 なぜか
基礎ができていない状態で高度機能を入れると、  
「勝ったように見えるけど説明できない」  
状態になりやすいから。

---

## 109. ML の扱い（完成品範囲外だが将来拡張のため固定しておく）

### 109.1 位置づけ
ML は主シグナルには使わない。  
使う場合は、
- veto
- size multiplier
- expected move estimator
に限定する。

### 109.2 理由
- feature drift 管理が必要
- low-cap perp の non-stationarity が強い
- 運用負荷が高い
- rule-based signal の何が効いているか見えにくくなる

### 109.3 将来の入り口
まずは deterministic ルールで十分に説明可能な performance が出た後に、
`edge_to_cost` の推定改善として ML を検討する。

---

## 110. 規模拡大ポリシー

### 110.1 口座規模拡大の原則
サイズ拡大は、期待値が確認できた後に行う。  
「小資金だから大きく張る」は禁止。

### 110.2 増額前に確認するもの
- lane 別の realized cost
- symbol 別 slippage deterioration
- depth ratio 悪化
- maker fill ratio の変化
- exposure 増で strategy edge が潰れないか

### 110.3 失敗条件
サイズを増やした結果
- cost gap が悪化
- missed maker が増加
- rescue 率が上昇
- DSR が落ちる  
なら増額は停止。

---

## 111. 個人開発としての保守性ルール

### 111.1 単純性優先
複雑だが検証不能な仕組みは採らない。  
複雑でも採るのは、**必要であり、監査可能なものだけ**。

### 111.2 可観測性優先
strategy の挙動は、あとから log と report で再構成できなければならない。

### 111.3 人手依存の最小化
手で見て判断するしかない箇所は、完成品の欠陥として扱う。

### 111.4 書き捨てコード禁止
研究コードであっても、本番候補に入るなら文書・型・テストを付ける。

---

## 112. Example YAML（実用レベル）

```yaml
app:
  name: StateStrike
  mode: tiny_live
  env: prod
  timezone: UTC
  instance_id: ss-prod-001

venue:
  name: hyperliquid
  network: mainnet
  enable_live: false
  isolated_only: true
  default_order_notional_usd: 100
  allowed_symbols: []
  denied_symbols: []

data:
  raw_root: /srv/statestrike/data/raw
  derived_root: /srv/statestrike/data/derived
  sqlite_path: /srv/statestrike/state/state.db
  partitioning: by_date_symbol
  retention_days_raw: 120
  retention_days_derived: 365
  profile_tick_multiplier: 4

collector:
  ws_ping_interval: 10
  reconnect_backoff_min_ms: 500
  reconnect_backoff_max_ms: 10000
  flush_interval_ms: 1000
  max_in_memory_events: 50000
  snapshot_recovery_enabled: true

universe:
  min_abs_ret_74h: 0.04
  min_volume_ratio_24: 1.15

profile:
  session_hours: 24
  micro_session_hours: 6
  value_area_pct: 0.70
  price_bucket_tick_mult: 4

state:
  displacement_atr_mult: 0.30
  displacement_spread_mult: 2.0
  displacement_impact_mult: 0.5
  acceptance_retrace_frac: 0.40
  post_liquidation_lock_bars: 3
  min_bars_for_acceptance: 1
  max_bars_for_acceptance: 2

tradability:
  expected_total_cost_bps_abs_max: 16
  impact_bps_abs_max: 6
  min_depth_ratio: 8.0
  edge_to_cost_core: 2.2
  edge_to_cost_reentry: 1.8
  edge_to_cost_fast: 1.6

funding:
  veto_minutes: 10
  penalty_minutes: 30
  severe_funding_z: 2.0
  penalty_funding_z: 1.2
  addon_disable_z: 1.5

shock:
  ret_1m_atr_mult: 2.0
  delta_oi_z_abs_min: 2.0
  spread_shock_z_min: 2.0
  lock_bars: 3

lanes:
  fast:
    enable_live: false
    risk_fraction: 0.0025
    score_min: 55
    max_hold_bars: 3
    rescue_allowed: true
  core:
    risk_fraction: 0.0035
    score_min: 65
    max_hold_bars: 12
    rescue_allowed: true
  reentry:
    risk_fraction: 0.0025
    score_min: 60
    max_hold_bars: 8
    max_entries_per_state: 2

sizing:
  use_empirical_kelly_cap: true
  kelly_cap_fraction: 0.25
  max_symbol_exposure_frac: 0.25
  max_total_open_risk_frac: 0.03
  min_notional_usd: 10

stop:
  atr_mult_core: 1.2
  atr_mult_reentry: 1.0
  atr_mult_fast: 0.8
  min_stop_bps: 8
  use_acceptance_level_override: true

addon:
  enable_live: true
  enable_stage_1: true
  enable_stage_2: false
  stage1_r_threshold: 0.5
  stage2_r_threshold: 1.0
  stage1_size_frac: 0.25
  stage2_size_frac: 0.15
  disable_near_funding_minutes: 20

exit:
  soft_ofi_decay_z: 0.0
  soft_basis_adverse_z: 1.5
  soft_edge_to_cost_min: 1.2
  soft_no_new_extreme_bars: 2
  force_flatten_before_funding_minutes: 10

risk:
  daily_stop_r: -4
  global_stop_r: -6
  strategy_dd_stop_r: -12
  cooldown_after_losses: 3
  cooldown_minutes: 720
  no_topup: true

validation:
  walkforward_windows: 3
  dsr_required: true
  dsr_min: 0.0
  min_closed_trades_for_live_eval: 100
  paper_days_required: 21
  tiny_live_days_required: 14

ops:
  alert_webhook: ""
  email_enabled: false
  daily_report_time_utc: "00:10"
  weekly_review_day_utc: "SUN"
  schedule_cancel_refresh_seconds: 30
```

---

## 113. 実装者向け判断基準

実装者が迷ったときは、次の順で判断する。

1. その変更は **isolated only** を崩さないか
2. その変更は **Core + Re-entry を live 本線とする構造**を崩さないか
3. その変更は **execution truth を軽視する方向**ではないか
4. その変更は **trial count を増やすだけで DSR を悪化**させないか
5. その変更は **後から audit できるか**

この 5 問で 2 個以上「怪しい」が出たら、実装を止めて仕様へ戻る。

---

## 114. 文書の終端

ここまでで、完成品としての StateStrike に必要な
- 戦略
- データ
- 執行
- リスク
- 監査
- 検証
- 運用
- 受入条件
が定義された。

以後の議論・実装・レビューは、本仕様書を source of truth として進める。  
本仕様書に書かれていない近道は、完成品の近道ではなく、ほとんどの場合 MVP への後退である。


## 115. 参考文献・公式資料一覧（採用根拠）

この章は、完成品仕様の背後にある外部一次情報・公式情報・代表研究を整理するためのものである。  
ここに書かれた文献は「そのまま閾値を借りる」ためではなく、**どの論点に外部根拠があり、どの論点が StateStrike 独自仮説かを明確にする**ために置いている。

### 115.1 公式情報

#### [O-1] Hyperliquid Docs: Order types
用途:
- ALO / IOC / GTC / Stop / Take / Scale / TWAP の存在
- maker-first / rescue policy の設計根拠
- TWAP を大口執行限定に留める根拠

StateStrike での使い道:
- execution policy
- emergency flatten 以外は market を常用しない方針
- lane 別 order style の制約

#### [O-2] Hyperliquid Docs: Funding
用途:
- funding が 1 時間ごと
- premium が 5 秒サンプル平均
- premium が impact/oracle 差に基づく
- funding 近接の state 分離の根拠

StateStrike での使い道:
- funding veto
- funding penalty
- funding distance bucket
- impact/oracle を feature 化する理由

#### [O-3] Hyperliquid Docs: Robust price indices / Oracle
用途:
- oracle / mark が約 3 秒ごと
- mark が margining, liquidations, TP/SL に使われる
- oracle が external spot weighted median であること

StateStrike での使い道:
- signal を trade last ではなく mark 基準に置く理由
- basis / mark-oracle change を confirmation に使う理由

#### [O-4] Hyperliquid Docs: Margining / Risks / Liquidations
用途:
- isolated/cross の違い
- maintenance margin と liquidation
- OI cap 制約
- order price/oracle 距離制約

StateStrike での使い道:
- isolated only
- no-topup
- OI cap 近傍の veto
- post-liquidation disorder state

#### [O-5] NautilusTrader Docs
用途:
- Hyperliquid integration
- L2/L3 execution-aware backtest
- liquidity_consumption
- high-level / low-level API 選択

StateStrike での使い道:
- execution truth
- backtest architecture
- current engine との compare design

#### [O-6] hftbacktest Docs
用途:
- replay 型 backtest の限界
- queue model
- partial fill simulation
- market impact 非再現の注意点

StateStrike での使い道:
- calibration only とする根拠
- maker fill band 推定
- latency / queue sensitivity の研究

### 115.2 研究論文

#### [R-1] Order flow and cryptocurrency returns (2026)
要点:
- world order flow が暗号資産 returns を説明・予測
- non-linear forecast の経済価値
- order flow の permanent effect

StateStrike での使い道:
- OFI を主役にする根拠
- flow persistence を add-on / soft exit に使う根拠
- order flow family を research priority に置く根拠

#### [R-2] Intraday return predictability in the cryptocurrency markets (2022)
要点:
- intraday momentum と reversal の両方が存在
- liquidity, jumps, FOMC などで性質が変化

StateStrike での使い道:
- M/L/X bucket
- shock state 分離
- Fast を本線にしない理由
- continuation を全銘柄に適用しない理由

#### [R-3] Perpetual Futures Contracts and Cryptocurrency Market Quality
要点:
- perpetual は volume を増やしうるが spread widening も伴う
- funding settlement hour と funding magnitude が market quality に影響

StateStrike での使い道:
- funding 近接 veto
- add-on 禁止帯
- edge-to-cost で funding distance を conditioning する理由

#### [R-4] Reconciling Open Interest with Traded Volume in Perpetual Swaps
要点:
- OI quality / reporting consistency への問題提起

StateStrike での使い道:
- OI を主 signal にしない根拠
- OI quality report を measurement layer の必須成果物に置く根拠

#### [R-5] Liquidation, Leverage and Optimal Margin in Bitcoin Futures Markets
要点:
- forced liquidations の大きさ
- average leverage の高さ
- tail の重さ

StateStrike での使い道:
- post-liquidation disorder の独立 state
- isolated only
- no-topup
- size-up の慎重運用

#### [R-6] The Deflated Sharpe Ratio
要点:
- selection bias
- backtest overfitting
- non-normality correction

StateStrike での使い道:
- DSR gate
- trial registry
- feature 追加時の採用制御

### 115.3 StateStrike 独自仮説
外部支持が比較的弱く、StateStrike 独自に検証すべきもの。

- auction transition / value migration の厳密優位
- micro-POC を使う re-entry の優位
- non-reentry の最適窓
- edge_to_cost の最適 horizon
- add-on 2 の live 既定値化

これらは **研究対象**であり、外部文献で「証明済み」と表現してはならない。

---

## 116. 変更管理テンプレート

仕様変更は、以下のテンプレートに従う。

### 116.1 変更票
- 変更 ID
- 変更対象章
- 変更カテゴリ（固定 / 仕様決定 / 設定値 / 研究仮説）
- 変更理由
- 外部根拠
- 期待効果
- 想定リスク
- rollback 条件
- 適用モード（research / paper / tiny-live / live）

### 116.2 変更レビュー項目
- live path へ影響するか
- DSR へ影響するか
- lane count を増減させるか
- cost audit に新列が必要か
- runbook 更新が必要か

### 116.3 緊急変更
緊急変更は
- severe incident の抑止
- key leakage
- venue API breaking change
に限定する。  
緊急変更でも変更票の事後記録は必須。

---

## 117. 最小実装順ではなく、完成品順で見たロードマップ

本仕様は完成品の仕様であり、最小実装順とは一致しない。  
ただし、完成品順で何を揃えるべきかを示しておく。

### 117.1 完成品の骨格
1. collector / raw / canonical
2. state engine
3. Core lane
4. Re-entry lane
5. execution layer
6. risk layer
7. audit layer
8. paper
9. tiny-live
10. Nautilus compare
11. DSR gate
12. production ops

### 117.2 後回しでよいもの
- Fast live
- post-liquidation live
- UI
- ML veto
- multi-venue

### 117.3 完成品を壊しやすい順
- live order path の安易な変更
- cost gate の緩和
- Fast lane の本番化
- OI の主シグナル化
- DSR gate の緩和

---

## 118. 仕様書の運用規則

### 118.1 source of truth
この文書は、少なくとも production v1.3 までの source of truth である。  
別資料があっても、矛盾時はこの文書を優先する。

### 118.2 例外
ただし、
- venue API が変更された
-セキュリティ事故
- critical production incident
のときは、緊急変更票により一時的に override できる。

### 118.3 実装との関係
コードはこの文書の実装であり、文書の代わりではない。  
レビュー時は「コードが動くか」より先に「この文書と一致するか」を確認する。

---

## 119. 最終まとめ（短縮版）

StateStrike 完成品仕様の核心は、次の 12 行で要約できる。

1. Hyperliquid only
2. isolated only
3. Core + Re-entry live only
4. Fast is research/paper only
5. post-liquidation continuation is forbidden
6. signal has L1/L2/L3 hierarchy
7. 15m state / 5m lane / 30s flow / 60s persistence
8. universe = 74h move + volume expansion
9. tradability and edge-to-cost are mandatory
10. OI is confirmation, funding is veto/penalty
11. Nautilus = execution truth, hftbacktest = calibration only
12. DSR + walk-forward + tiny-live are required before calling it “good”

この 12 行を崩す変更は、完成品の思想そのものを変える。


## 120. 参考 URL 一覧

以下は 115 章で言及した主要な一次情報・公式資料・代表研究の URL 一覧である。  
運用時は URL そのものよりも、タイトル・取得日・内容要約を基準に参照すること。

### 120.1 Hyperliquid 公式
- Order Types  
  https://hyperliquid.gitbook.io/hyperliquid-docs/trading/order-types
- Funding  
  https://hyperliquid.gitbook.io/hyperliquid-docs/trading/funding
- Margining  
  https://hyperliquid.gitbook.io/hyperliquid-docs/trading/margining
- Robust Price Indices  
  https://hyperliquid.gitbook.io/hyperliquid-docs/trading/robust-price-indices
- Oracle  
  https://hyperliquid.gitbook.io/hyperliquid-docs/hypercore/oracle
- Risks  
  https://hyperliquid.gitbook.io/hyperliquid-docs/risks
- Liquidations  
  https://hyperliquid.gitbook.io/hyperliquid-docs/trading/liquidations
- Fees  
  https://hyperliquid.gitbook.io/hyperliquid-docs/trading/fees
- WebSocket  
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket
- WebSocket Subscriptions  
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/websocket/subscriptions
- Contract Specifications  
  https://hyperliquid.gitbook.io/hyperliquid-docs/trading/contract-specifications
- Error Responses  
  https://hyperliquid.gitbook.io/hyperliquid-docs/for-developers/api/error-responses

### 120.2 OSS
- NautilusTrader Hyperliquid Integration  
  https://nautilustrader.io/docs/latest/integrations/hyperliquid
- NautilusTrader Backtesting  
  https://nautilustrader.io/docs/latest/concepts/backtesting
- hftbacktest Order Fill  
  https://hft.readthedocs.io/en/latest/order_fill.html
- Hyperliquid Python SDK  
  https://github.com/hyperliquid-dex/hyperliquid-python-sdk
- marketprofile (PyPI)  
  https://pypi.org/project/marketprofile/
- deflated-sharpe (PyPI)  
  https://pypi.org/project/deflated-sharpe/

### 120.3 研究
- Order flow and cryptocurrency returns  
  https://www.sciencedirect.com/science/article/pii/S1386418126000029
- Intraday return predictability in the cryptocurrency markets: Momentum, reversal, or both  
  https://www.sciencedirect.com/science/article/pii/S1062940822000833
- Perpetual Futures Contracts and Cryptocurrency Market Quality  
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=4218907
- Reconciling Open Interest with Traded Volume in Perpetual Swaps  
  https://ledgerjournal.org/ojs/ledger/article/view/325
- Liquidation, Leverage and Optimal Margin in Bitcoin Futures Markets  
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=3781646
- The Deflated Sharpe Ratio: Correcting for Selection Bias, Backtest Overfitting and Non-Normality  
  https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2460551
