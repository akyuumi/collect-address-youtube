#!/bin/bash

# 仮想環境の有効化と初回セットアップ
if [ -d "venv" ]; then
  source venv/bin/activate
else
  python3 -m venv venv
  source venv/bin/activate
  pip install --upgrade pip
  pip install -r requirements.txt
fi

# ログディレクトリとファイルの指定
LOG_DIR="./logs"
LOG_FILE="$LOG_DIR/batch_$(date '+%Y%m%d').log"

# ログディレクトリがなければ作成
mkdir -p "$LOG_DIR"

# 実行開始ログ
echo "[$(date '+%Y-%m-%d %H:%M:%S')] バッチ処理開始" >> "$LOG_FILE"

# Pythonスクリプト実行
python youtube_to_sheets.py >> "$LOG_FILE" 2>&1

# 終了ステータス取得
STATUS=$?

# 実行終了ログ
if [ $STATUS -eq 0 ]; then
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] バッチ処理正常終了" >> "$LOG_FILE"
else
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] バッチ処理異常終了 (status=$STATUS)" >> "$LOG_FILE"
fi %  