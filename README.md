# fx-practice-data

[FX練習チャート](https://github.com/katokohei-khd/fx-practice-app) が読み込む価格データです。
Dukascopy 社の公開している過去データ（1分足）を15分足にまとめたもので、個人の練習用です。

- `data/{銘柄}/{年}.bin`、`data/{銘柄}/{年-月}.bin`：15分足（形式は `scripts/build.py` の説明を参照）
- `data/index.json`：ファイル一覧と各銘柄のデータ期間
- `data/rate.json`：円換算用のドル円日足終値

## 更新
- `Daily update`：毎日10:23（日本時間）に前日分を追加します。
- `Backfill`：過去分を一括で取り込み直します（手動実行）。

どちらも履歴を残さず1コミットで上書きするので、リポジトリは大きくなりません。
