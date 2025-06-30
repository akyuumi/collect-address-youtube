# YouTubeチャンネル情報収集ツール

## 概要

このツールは、指定したYouTubeのカテゴリに含まれる人気動画を元に、条件に合致するチャンネルの情報を自動で収集し、Googleスプレッドシートに記録するためのPythonスクリプトです。
収集結果はSlackにも通知されます。

## 主な機能

-   **チャンネル情報の自動収集**: 指定したカテゴリの人気動画からチャンネル情報を取得します。
-   **フィルタリング**: チャンネル登録者数に基づき、指定した数に満たないチャンネルを収集対象から除外します（デフォルトは10万人以上）。
-   **メールアドレス抽出**: チャンネルの概要欄から正規表現でメールアドレスを抽出し、見つからない場合は「取得失敗」と記録します。
-   **Googleスプレッドシートへの出力**: 収集したチャンネル情報をGoogleスプレッドシートに追記します。
    -   **重複排除**: 既にシートに存在するチャンネルは追加せず、新規のチャンネルのみを追記します。
    -   **日本語ヘッダー**: ヘッダー（項目名）は日本語で出力されます。
-   **Slack通知**: 実行完了後、新規に取得したチャンネル数やその詳細をSlackに通知します。

## 必要なもの

-   Python 3.x
-   Google Cloud Platform (GCP) プロジェクト
    -   YouTube Data API v3 の有効化
    -   Google Sheets API の有効化
    -   サービスアカウントキー (JSON形式)
-   SlackワークスペースとIncoming Webhook URL

## セットアップ手順

1.  **リポジトリのクローン**
    ```bash
    git clone <repository_url>
    cd collect-address-youtube
    ```

2.  **必要なライブラリのインストール**
    ```bash
    pip install -r requirements.txt
    ```

3.  **環境変数の設定**
    プロジェクトのルートディレクトリに `.env` ファイルを作成し、以下の内容を記述します。

    ```env
    # Google
    YOUTUBE_API_KEY="ここにYouTube APIキーを入力"
    GOOGLE_SERVICE_ACCOUNT_KEY='ここにサービスアカウントのJSONキーを貼り付け'
    SPREADSHEET_ID="ここにGoogleスプレッドシートのIDを入力"

    # Slack
    SLACK_WEBHOOK_URL="ここにSlackのIncoming Webhook URLを入力"

    # オプション
    MIN_SUBSCRIBER_COUNT=100000
    SHEET_NAME="Sheet1"
    ```
    -   `GOOGLE_SERVICE_ACCOUNT_KEY`: ダウンロードしたJSONキーの中身を**一行の文字列として**コピー＆ペーストしてください。
    -   `SPREADSHEET_ID`: GoogleスプレッドシートのURL `https://docs.google.com/spreadsheets/d/SPREADSHEET_ID/edit` の `SPREADSHEET_ID` の部分です。
    -   **重要**: 作成したサービスアカウントのメールアドレスを、出力先のGoogleスプレッドシートの共有設定で「編集者」として追加してください。

4.  **収集カテゴリの設定**
    `config/category_ids.json` を編集して、情報収集の対象としたいYouTubeチャンネルのカテゴリIDを追加・変更します。

    ```json
    {
        "categories": [
            { "id": "1", "name": "映画とアニメ" },
            { "id": "2", "name": "自動車と乗り物" },
            { "id": "10", "name": "音楽" }
        ]
    }
    ```

## 実行方法

セットアップ完了後、以下のコマンドでスクリプトを実行します。

```bash
python youtube_to_sheets.py
```

スクリプトが正常に完了すると、指定したGoogleスプレッドシートに新しいチャンネル情報が追記され、Slackに通知が送信されます。