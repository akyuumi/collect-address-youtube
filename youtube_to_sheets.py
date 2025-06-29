import os
import json
import logging
import time
import re
import pandas as pd
from datetime import datetime
from typing import List, Dict, Set

from dotenv import load_dotenv
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# ロギングの設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# 環境変数の読み込み
load_dotenv()
API_KEY = os.getenv('YOUTUBE_API_KEY')
MIN_SUBSCRIBER_COUNT = int(os.getenv('MIN_SUBSCRIBER_COUNT', '100000'))  # 10万未満除外

# Google Sheets APIのスコープ
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def extract_email(description: str) -> str:
    """説明文からメールアドレスを抽出"""
    if not description:
        return "取得失敗"
    
    # メールアドレスのパターン
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}'
    match = re.search(email_pattern, description)
    
    if match:
        return match.group(0)
    return "取得失敗"

class YouTubeChannelCollector:
    def __init__(self):
        self.youtube = build('youtube', 'v3', developerKey=API_KEY)
        self.existing_channels = set() # スプレッドシートに書き出す際に重複チェックを行うため、既存チャンネルを保持
        self.sheets_service = self._authenticate_google_sheets()
        
    def _authenticate_google_sheets(self):
        creds = None
        # token.jsonは、ユーザーのアクセストークンとリフレッシュトークンを保存します。
        # 初回認証時に自動的に作成されます。
        if os.path.exists('token.json'):
            creds = Credentials.from_authorized_user_file('token.json', SCOPES)
        
        # 認証情報がない、または期限切れの場合
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials.json', SCOPES)
                creds = flow.run_local_server(port=0)
            # 次回のために認証情報を保存
            with open('token.json', 'w') as token:
                token.write(creds.to_json())
        
        return build('sheets', 'v4', credentials=creds)

    def _load_category_ids(self) -> List[Dict]:
        """カテゴリIDの設定を読み込み"""
        # sample-bat/config/category_ids.json を参照
        # このファイルは、youtube_to_sheets.py と同じ階層に配置するか、パスを適切に設定する必要があります。
        # 今回は、youtube_to_sheets.py と同じ階層に config ディレクトリを作成し、その中に配置する前提とします。
        try:
            with open('config/category_ids.json', 'r', encoding='utf-8') as f:
                return json.load(f)['categories']
        except FileNotFoundError:
            logger.error("config/category_ids.json が見つかりません。")
            return []
        except json.JSONDecodeError:
            logger.error("config/category_ids.json の形式が不正です。")
            return []
    
    def get_popular_videos(self, category_id: str) -> List[str]:
        """人気動画からチャンネルIDを取得"""
        try:
            channel_ids = set()
            next_page_token = None
            daily_limit = 10000  # YouTube Data APIの1日のクォータ制限
            total_quota = 0
            
            while True:
                request = self.youtube.videos().list(
                    part='snippet',
                    chart='mostPopular',
                    regionCode='JP',
                    videoCategoryId=category_id,
                    maxResults=50,
                    pageToken=next_page_token
                )
                response = request.execute()
                
                # クォータ消費量の計算（videos.listは1リクエストあたり1クォータ）
                total_quota += 1
                
                for item in response.get('items', []):
                    channel_id = item['snippet']['channelId']
                    if channel_id not in self.existing_channels: # スプレッドシートに書き出す際に重複チェックを行うため、既存チャンネルを保持
                        channel_ids.add(channel_id)
                
                # 次のページのトークンを取得
                next_page_token = response.get('nextPageToken')
                
                # 次のページがない場合、またはクォータ制限に達した場合は終了
                if not next_page_token or total_quota >= daily_limit:
                    break
                
                # API制限を考慮して少し待機
                time.sleep(1)
            
            logger.info(f"カテゴリID[{category_id}]で{len(channel_ids)}件のチャンネルを取得しました。")
            return list(channel_ids)
        except Exception as e:
            logger.error(f"動画の取得に失敗しました。カテゴリID[{category_id}]: {str(e)}")
            return []
    
    def get_channel_details(self, channel_ids: List[str]) -> List[Dict]:
        """チャンネル詳細情報を取得"""
        if not channel_ids:
            return []
        
        channels = []
        # チャンネルIDを50個ずつのバッチに分割
        batch_size = 50
        for i in range(0, len(channel_ids), batch_size):
            batch = channel_ids[i:i + batch_size]
            try:
                request = self.youtube.channels().list(
                    part='snippet,statistics',
                    id=','.join(batch),
                    maxResults=batch_size
                )
                response = request.execute()
                
                for item in response.get('items', []):
                    description = item['snippet'].get('description', '')
                    subscriber_count = int(item['statistics'].get('subscriberCount', 0))
                    if subscriber_count < MIN_SUBSCRIBER_COUNT:
                        continue  # 10万未満は除外
                    channel = {
                        'channel_id': item['id'],
                        'title': item['snippet']['title'],
                        'description': description,
                        'email': extract_email(description),
                        'subscriber_count': subscriber_count,
                        'view_count': int(item['statistics'].get('viewCount', 0)),
                        'video_count': int(item['statistics'].get('videoCount', 0)),
                        'fetched_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S') # スプレッドシート用に文字列化
                    }
                    channels.append(channel)
                
                # API制限を考慮して少し待機
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"チャンネル詳細の取得に失敗しました。バッチ {i//batch_size + 1}: {str(e)}")
                continue
        
        return channels
    
    def write_to_spreadsheet(self, data: List[Dict], spreadsheet_id: str, range_name: str = 'Sheet1!A1'):
        """データをGoogleスプレッドシートに書き込む"""
        if not data:
            logger.info("書き込むデータがありません。")
            return

        # ヘッダー行
        headers = list(data[0].keys())
        
        # データ行
        values = [list(d.values()) for d in data]

        body = {
            'values': [headers] + values
        }
        
        try:
            # スプレッドシートに書き込み
            result = self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id, 
                range=range_name,
                valueInputOption='RAW', 
                body=body
            ).execute()
            logger.info(f"{result.get('updatedCells')} セルが更新されました。")
            logger.info(f"スプレッドシートにデータを書き込みました: {spreadsheet_id}")
        except Exception as e:
            logger.error(f"スプレッドシートへの書き込みに失敗しました: {str(e)}")

    def run(self, spreadsheet_id: str):
        """メイン処理の実行"""
        logger.info(f"バッチ処理を開始します。")
        
        # カテゴリIDの読み込み
        categories = self._load_category_ids()
        all_new_channels = []
        
        # カテゴリごとに処理
        for category in categories:
            logger.info(f"処理中 カテゴリ: {category['name']} (ID: {category['id']})")
            
            # 人気動画からチャンネルIDを取得
            channel_ids = self.get_popular_videos(category['id'])
            
            if channel_ids:
                # チャンネル詳細を取得
                channels: List[Dict] = self.get_channel_details(channel_ids)
                all_new_channels.extend(channels)
                
                new_channels_count = len(channels)
                logger.info(f"Fetched {new_channels_count} new channels in category {category['name']}")
            
            # API制限を考慮して少し待機
            time.sleep(1)
        
        logger.info(f"処理が完了しました。合計取得チャンネル数: {len(all_new_channels)}")
        
        # スプレッドシートに書き込み
        if all_new_channels:
            self.write_to_spreadsheet(all_new_channels, spreadsheet_id)
        else:
            logger.info("新規チャンネルが取得されなかったため、スプレッドシートへの書き込みはスキップされました。")

if __name__ == '__main__':
    if not API_KEY:
        raise ValueError("YouTube APIキーが設定されていません。")

    # スプレッドシートIDを環境変数から取得
    SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
    if not SPREADSHEET_ID:
        raise ValueError("SPREADSHEET_IDが設定されていません。")

    collector = YouTubeChannelCollector()
    collector.run(SPREADSHEET_ID)
