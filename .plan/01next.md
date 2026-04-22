# StateStrike 次工程 指示書（Phase 1 完了後）

## 結論
次に進むべきは **Phase 1.5: mainnet 実データ smoke + 品質監査の実運用化** です。

**まだ Phase 2 の execution/live path 実装には進まない。**
理由は、現時点では fixture replay では end-to-end が通っているが、
- 実ネットワークでの収集安定性
- gap / reconnect / skew / quarantine の実挙動
- Nautilus compare-ready export の実データ妥当性
が未確認だからです。

## 今ここで固定する決定
1. この repo を **current engine 兼 canonical 実装** とする。
2. パッケージルートは **`src/statestrike/`** で固定する。
3. 現段階は **Python-only** で進める。Rust 化は検討対象にしない。
4. 次マイルストーンは **collector + raw/normalized/quarantine/audit/export を実データで安定稼働** とする。
5. **live 注文経路は未着手のまま維持**する。
6. symbol universe はまず **少数 allowlist（6〜10 銘柄）** で始める。
7. `true profile` はまだ入れない。**Lite fallback のまま**進める。
8. `add-on` はまだ入れない。**paper 以降**で扱う。
9. `Re-entry` は tiny-live までは **最大 1 回** に抑える。
10. `Fast` は引き続き **paper/backtest 専用**。
11. quality gate は **Pandera を normalized 入口で強制**する。
12. `DSR` / `trial registry` はインターフェースだけ先に固定し、実値判定は後続で入れる。

## 次に実装するもの

### A. mainnet smoke collector
目的:
- 実ネットワークで collector が壊れないか確認する
- fixture では出ない gap / reconnect / malformed payload を観測する

対象:
- `l2_book`
- `trades`
- `active_asset_ctx`
- 補助として `candle`

運用条件:
- 6〜10 銘柄 allowlist
- 48〜72 時間連続運転
- raw / normalized / quarantine / audit / export まで日次で生成

完了条件:
- collector が落ちない
- reconnect 後も manifest が壊れない
- normalized と quarantine の分離が安定
- audit が毎回出る

### B. 実データ品質監査の強化
目的:
- fixture replay では見えない破損や時刻異常を確実に落とす

追加すべき監査:
- exchange_ts と recv_ts の skew 分布
- reconnect epoch ごとの gap 件数
- snapshot 再取得後の book continuity
- symbol ごとの quarantine 率
- asset_ctx stale 率
- crossed book の持続時間

完了条件:
- 日次レポートで異常の有無が見える
- severe / warning の閾値が定義される
- quarantine 発生時の再処理手順が決まる

### C. export 妥当性確認
目的:
- compare-ready bundle が実データで downstream に渡せるか確認する

やること:
- Nautilus export parquet を実データで生成
- hftbacktest 用 `.npz` を実データで生成
- row count / ts continuity / symbol count / null count を比較

完了条件:
- export 成功
- row count の説明可能性あり
- downstream 用の minimum schema が確定

## この段階でやらないこと
- live 注文
- paper runner
- tiny-live
- user stream 依存の live reconciliation
- add-on
- true profile
- GRVT 対応
- ML veto / sizing

## 成功基準
以下を満たしたら、Phase 1.5 完了としてよい。

1. mainnet 実データで 48〜72 時間 collector が安定稼働
2. raw / normalized / quarantine / audit / export がすべて生成される
3. quarantine の主要原因が分類できる
4. audit の severe / warning 閾値が定義される
5. Nautilus compare-ready export が実データで通る
6. 既存 fixture テストが引き続き green

## 失敗時の優先順位
1. collector の安定性
2. timestamp / gap / dedup の整合性
3. quarantine の妥当性
4. export の妥当性
5. パフォーマンス最適化

## その後の進み方
Phase 1.5 完了後に、初めて **Phase 2: execution/live path 設計と実装** に進む。
順番は以下。
1. Nautilus bridge 実装
2. current engine vs Nautilus compare report
3. paper execution path
4. tiny-live

## 指示
上記方針で進めること。
迷った場合は、
- 実データの整合性
- 後から比較できること
- live path をまだ入れないこと
を優先する。
