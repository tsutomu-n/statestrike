# StateStrike 実装前確認事項・不足情報リスト v1

- 作成日: 2026-04-22 JST
- ステータス: 確認待ち
- 対象読者: オーナー、実装責任者、検証責任者、運用責任者
- 目的: `0422statestrike_senior_se_design_dossier_v1.md` を基準に実装を始めるにあたり、先に確認すべき質問と、まだ不足している実装情報を整理する
- 前提: 設計の source of truth は `0422statestrike_senior_se_design_dossier_v1.md`

---

## 0. この文書の使い方

この文書は、仕様をひっくり返すためのものではない。  
**既に固定済みの事項は再質問せず、未固定または実装粒度に落ちていない項目だけを確認する**ための文書である。

優先度は次の 3 段階で扱う。

- `Blocker`: 回答がないと実装境界や責務がぶれやすい
- `Needed Soon`: 最初の実装は始められるが、途中で必要になる
- `Later`: 初期実装後でもよいが、paper / tiny-live / live までには必要

---

## 1. 既に固定済みで、再質問しなくてよいこと

以下は設計資料で固定済みなので、実装者が勝手に再オープンしない。

- Hyperliquid only
- Perpetuals only
- isolated only
- live 本線は Core + Re-entry
- Fast は research / paper 専用
- WS 主、REST / S3 従
- raw と normalized を分け、normalized 前段に Pandera gate を置く
- Pybotters = data truth
- NautilusTrader = execution truth
- hftbacktest = queue / latency calibration truth
- DuckDB + Parquet = 分析 spine
- DSR + walk-forward + paper + tiny-live を採用ゲートに含める

---

## 2. 最優先で確認したいこと

### 2.1 実装スコープの初期ゴール

1. この repo の最初の到達点はどこか。
   - 候補:
   - collector + raw/normalized + Pandera gate まで
   - 上記 + derived / audit まで
   - 上記 + Nautilus export まで
   - 上記 + paper runner まで
   - 上記 + tiny-live まで
   - 理由: 設計資料は完成品全体を定義しているが、最初のマイルストーンが未固定だと実装が過剰に広がる
   - 優先度: `Blocker`

2. 今回この repo で「live 注文経路」まで作るか、それとも当面は paper / backtest までに限定するか。
   - 理由: secrets、署名、reconciliation、runbook、review 厳格度が大きく変わる
   - 優先度: `Blocker`

3. 現在の「current engine」は既に別 repo / 別コードとして存在するか。
   - あるなら場所、言語、参照方法、compare report の比較対象として使ってよいか
   - ないなら、この repo が current engine 兼本線実装になるのか
   - 理由: 設計資料では current engine と Nautilus の併存が前提になっている
   - 優先度: `Blocker`

### 2.2 コードベースの骨格

4. パッケージルート名を何に固定するか。
   - 候補:
   - `src/statestrike/`
   - `src/perpscope/`
   - 別名
   - 理由: 設計資料では `src/perpscope/` の例がある一方、製品名は StateStrike。ここを後から変えると import と文書が広範囲に壊れる
   - 優先度: `Blocker`

5. Python-only で始めるか、collector の一部を初期から Rust 候補として残すか。
   - 理由: 境界が曖昧だと premature optimization か、逆に移植前提を無視した設計になりやすい
   - 優先度: `Needed Soon`

6. 依存バージョン固定の方針をどこまで厳格にするか。
   - 特に `pybotters`、`pandera`、`duckdb`、`nautilus_trader`、`hftbacktest`、`hyperliquid-python-sdk`
   - 理由: collector / execution / schema 周辺は upstream 変更の影響が大きい
   - 優先度: `Needed Soon`

### 2.3 運用口座と環境

7. Hyperliquid の利用ネットワークは初期から mainnet 前提か、testnet / sandbox 相当を使う期間を設けるか。
   - 理由: live path のテスト戦略が変わる
   - 優先度: `Blocker`

8. strategy wallet / reserve wallet / ops wallet の実際の分離方針と、運用上の口座数はどうするか。
   - 理由: live path の設定、署名経路、資金保全設計に直結する
   - 優先度: `Needed Soon`

9. secrets の保管方法を何で固定するか。
   - 候補:
   - `.env`
   - systemd EnvironmentFile
   - 既存 secret manager
   - 理由: 署名 path とデプロイ手順に直結する
   - 優先度: `Blocker`

10. 通知先を何にするか。
   - 候補:
   - webhook
   - email
   - Discord / Slack / Telegram など
   - 理由: critical alert と warning alert の設計が変わる
   - 優先度: `Needed Soon`

---

## 3. データ基盤で不足している情報

### 3.1 collector / raw / normalized

11. `l2_book` の normalized 表現を何にするか。
   - 候補:
   - 1 message = 複数 level 行へ展開
   - 1 message = bids/asks 配列を保持
   - snapshot と delta を別 schema に分ける
   - 理由: Nautilus export、book sanity、impact 推定の実装コストが大きく変わる
   - 優先度: `Blocker`

12. channel ごとの primary key / dedup key をどう定義するか。
   - `trades`, `l2_book`, `active_asset_ctx`, `user_fills`, `order_updates` ごとに必要
   - 理由: Pandera gate と idempotency の実装に必須
   - 優先度: `Blocker`

13. `capture_session_id` の採番規則をどうするか。
   - 例:
   - collector 起動単位
   - reconnect epoch を内包
   - 日次ローテーション込み
   - 理由: raw/normalized/audit の追跡性に関わる
   - 優先度: `Needed Soon`

14. `book_epoch` の意味と切り替え条件をどう固定するか。
   - snapshot 再受信時
   - reconnect 後
   - gap repair 後
   - 理由: book continuity 判定と gap audit に必要
   - 優先度: `Needed Soon`

15. recoverable gap / non-recoverable gap の閾値を channel ごとにどう置くか。
   - 理由: silent data corruption を防ぐには、実装前に判定基準が必要
   - 優先度: `Blocker`

16. official Info / S3 / Tardis の利用優先順を、初期実装でどこまで入れるか。
   - 理由: gap repair と bootstrap の責務がぶれる
   - 優先度: `Needed Soon`

17. raw / normalized / derived の retention 日数、圧縮方式、ディスク予算をどう置くか。
   - 理由: collector 設計と batch 設計の前提になる
   - 優先度: `Needed Soon`

18. 初期の symbol universe は何銘柄程度を想定するか。
   - 全 perp
   - allowlist 数銘柄
   - 流動性上位のみ
   - 理由: subscription budget、CPU/RAM、batch 時間に影響する
   - 優先度: `Blocker`

### 3.2 canonical / derived

19. `active_asset_ctx` から canonical `asset_ctx` へ写す exact field mapping を固定したい。
   - 必須:
   - `mark_px`
   - `oracle_px`
   - `funding_rate`
   - `open_interest`
   - `mid_px`
   - 任意:
   - `impact_bid_px`
   - `impact_ask_px`
   - `next_funding_ts`
   - 理由: funding/OI audit と signal 入力で列不足が起きやすい
   - 優先度: `Needed Soon`

20. profile 実装の初期方針はどちらか。
   - true profile を最初から入れる
   - Lite fallback で先に通し、後で true profile へ置換する
   - 理由: features/state の開発順が大きく変わる
   - 優先度: `Blocker`

21. `expected_total_cost_bps` の初期版をどこまで精密に作るか。
   - 最低限:
   - fee
   - spread crossing
   - impact
   - rescue penalty
   - 追加候補:
   - missed maker -> taker fallback
   - time-of-hour conditioning
   - symbol bucket conditioning
   - 理由: Core / Re-entry の gate に直結する
   - 優先度: `Blocker`

22. `maker_fill_proxy` / `expected_mfe_conditional` / `edge_to_cost` の初期実装はどの程度の proxy を許すか。
   - 理由: ここを曖昧にすると L3 判定が仕様だけ存在して実装できない
   - 優先度: `Blocker`

23. OI / funding quality report の初期版で、最低限どの指標まで出せばよいか。
   - 理由: 設計上は必須成果物だが、実装粒度が未定
   - 優先度: `Needed Soon`

---

## 4. 戦略・シグナル実装で不足している情報

24. `universe` を live / paper / backtest で完全共通にするか、mode ごとに allowlist 上書きを許すか。
   - 理由: 再現性と実運用都合のバランスに関わる
   - 優先度: `Needed Soon`

25. `score` の実装方式をどうするか。
   - 候補:
   - 重み付き線形 score
   - additive penalty + hard gate
   - ルールベース離散 score
   - 理由: Core / Re-entry / Fast の境界が曖昧になりやすい
   - 優先度: `Blocker`

26. `delta_oi_60s` や `basis_change_30s` の標準化方式を何に固定するか。
   - rolling z-score
   - symbol-aware robust scale
   - 別方式
   - 理由: symbol 間比較と gate 安定性に直結する
   - 優先度: `Needed Soon`

27. `POST_LIQUIDATION_DISORDER` の lock 期間は初期値をそのまま採用するか。
   - 例: `lock_bars = 3`
   - 理由: state machine の分岐で早期に必要
   - 優先度: `Needed Soon`

28. Re-entry の最大回数は初期から 2 回にするか、tiny-live までは 1 回固定にするか。
   - 理由: 実装複雑性と exposure 管理に直結する
   - 優先度: `Needed Soon`

29. add-on は初期実装に含めるか。
   - 候補:
   - stage1 まで含める
   - 仕様だけ残して後回し
   - 完全に phase 2 へ回す
   - 理由: bracket 再計算と risk 会計が一気に複雑になる
   - 優先度: `Blocker`

30. Short 側を初期から完全対称実装するか、まず Long/Short 共通骨格を作って両方向テストまで一括で入れるか。
   - 理由: 片側だけ先に作ると後で条件分岐が歪みやすい
   - 優先度: `Needed Soon`

---

## 5. 執行・リスク・live path で不足している情報

31. `client_order_id` の deterministic format を何にするか。
   - 例:
   - `{run_id}-{symbol}-{lane}-{ts}-{seq}`
   - 理由: intent/fill/reconciliation の中核
   - 優先度: `Blocker`

32. bracket attach の責務をどこに置くか。
   - fill event 後すぐ execution layer で付ける
   - dedicated reconciliation worker が担う
   - 理由: missing bracket incident を防ぐ設計が変わる
   - 優先度: `Blocker`

33. `ALO -> IOC rescue` の猶予時間、最大サイズ、再試行回数をどう置くか。
   - 理由: cost audit と execution route の挙動が変わる
   - 優先度: `Needed Soon`

34. `schedule cancel` の実際の更新周期と、失敗時の stop 条件をどう固定するか。
   - 例 YAML では 30 秒
   - 理由: live guard の必須要件
   - 優先度: `Needed Soon`

35. `global stop` / `daily stop` / `strategy stop` / `cooldown` の state 保存先をどこに置くか。
   - SQLite
   - ファイル
   - 別 durable store
   - 理由: restart-safe 要件に関わる
   - 優先度: `Needed Soon`

36. live path の flatten 優先順位をどう定義するか。
   - market 禁止が原則でも、`EMERGENCY_FLATTEN` 時の order style は明示が必要
   - 理由: severe incident 時に迷うと危険
   - 優先度: `Needed Soon`

37. fee tier / min notional / tick / lot / leverage tier / OI cap の metadata をどこから取得し、いつ更新するか。
   - 理由: order pre-check と rounding の実装に必須
   - 優先度: `Blocker`

38. paper mode の fill モデルは、初期から optimistic/base/pessimistic 3 モードを持つか、それとも base のみで始めるか。
   - 理由: paper 実装を早く出すか、最初から校正前提で作るかの判断になる
   - 優先度: `Needed Soon`

---

## 6. validation / audit / ops で不足している情報

39. DSR の閾値を最初にいくつで運用するか。
   - 例 YAML では `0.0`
   - これを暫定値として採用するか、別閾値を置くか
   - 理由: acceptance gate のコード化に必要
   - 優先度: `Needed Soon`

40. `trial registry` の保存先を何にするか。
   - CSV
   - SQLite
   - Markdown + JSON
   - 理由: 実験管理が宙に浮きやすい
   - 優先度: `Needed Soon`

41. compare report の「説明可能」の判定を何で置くか。
   - 例:
   - fill count 差
   - maker/taker mix 差
   - realized/expected cost 差
   - lane contribution 差
   - 理由: compare report を作っても、採否判断ができないと意味がない
   - 優先度: `Needed Soon`

42. incident journal の保管先と retention をどうするか。
   - 理由: auditability と weekly review に必要
   - 優先度: `Needed Soon`

43. daily / weekly report を最初から自動生成するか、初期は CLI 生成でよいか。
   - 理由: ops 実装の優先順位に関わる
   - 優先度: `Later`

44. 受入テストをどの段階から CI に載せるか。
   - data acceptance
   - schema gate
   - export
   - paper flow
   - 理由: 完成品要件は重いので、どこから自動化するかを決めたい
   - 優先度: `Needed Soon`

45. release channel を repo / config / artifact 上でどう表現するか。
   - `research`, `paper`, `tiny-live`, `live-stable`
   - 理由: 誤配備防止に必要
   - 優先度: `Later`

---

## 7. 実装順に沿った追加確認

### 7.1 collector 着手前

- パッケージ構成
- symbol universe 規模
- raw / normalized schema
- dedup key
- gap 判定基準
- retention / disk 予算

### 7.2 derived / state 着手前

- true profile vs lite fallback
- cost surface 初期版の粒度
- score の方式
- edge_to_cost proxy
- OI/funding quality report 最低要件

### 7.3 execution / paper 着手前

- current engine の所在
- order ID format
- bracket attach の責務
- rescue policy
- metadata refresh の方式
- paper fill モデル

### 7.4 tiny-live / live 着手前

- secrets 保管方式
- wallet segregation
- alert channel
- schedule cancel 実運用値
- flatten policy
- weekly review / change ticket の運用先

---

## 8. 回答をもらえない場合の暫定方針

回答待ちで全作業を止めないため、未回答時は次の保守的前提で進める案を置く。

1. 初期マイルストーンは `collector + raw/normalized + Pandera gate + minimal derived audit` までとする
2. live 注文経路は phase 1 では作らず、paper / export までに留める
3. パッケージルートは `src/statestrike/` を採用する
4. 初期実装は Python-only とする
5. symbol universe は allowlist 少数銘柄から始める
6. true profile が重ければ、一時的に Lite fallback を使うが、state API は true profile へ差し替え可能に設計する
7. add-on は phase 1 から外す
8. Re-entry 最大回数は tiny-live までは 1 回に抑える
9. paper fill は初期は base モード中心にし、calibration 接続点だけ先に作る
10. DSR / trial registry / compare report はインターフェースを先に固定し、中身は段階的に埋める

---

## 9. 実装者から最初に投げるべき質問一覧

最初の打ち合わせでは、少なくとも次の 12 問に答えが欲しい。

1. この repo の初期マイルストーンはどこまでか
2. live 注文経路まで今回含めるか
3. current engine はどこにあるか
4. パッケージルート名は何に固定するか
5. 初期 symbol universe は何銘柄を想定するか
6. `l2_book` normalized schema をどう表現するか
7. dedup key / primary key を channel ごとにどう置くか
8. gap 判定基準をどう置くか
9. true profile を最初からやるか、Lite fallback で先に進むか
10. `expected_total_cost_bps` と `edge_to_cost` の初期 proxy をどこまで許すか
11. add-on を初期実装に含めるか
12. secrets / wallet / alert の実運用前提を何で固定するか

---

## 10. 最後に

この文書の意図は、仕様を弱めることではない。  
むしろ、**設計資料で固定済みの思想を壊さずに実装へ落とすため、未固定箇所だけを早めに潰す**ことにある。

StateStrike は「どの機能を足すか」より前に、

- 何を最初の到達点にするか
- 何を source of truth にするか
- どこまでを今すぐ作るか
- どの値を暫定値として許すか

を明示してから着手した方が壊れにくい。
