import os
import json
import logging
import time
import re
import requests
import pandas as pd
from datetime import datetime
from typing import List, Dict, Set

from dotenv import load_dotenv
from googleapiclient.discovery import build
from google.oauth2 import service_account
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

# Google Sheets APIのスコープ
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

def extract_email(description: str) -> str:
    """説明文からメールアドレスを抽出"""
    if not description:
        return "取得失敗"
    
    # メールアドレスのパターン
    email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
    match = re.search(email_pattern, description)
    
    if match:
        return match.group(0)
    return "取得失敗"

class YouTubeChannelCollector:
    def __init__(self, spreadsheet_id: str, sheet_name: str):
        self.youtube = build('youtube', 'v3', developerKey=YOUTUBE_API_KEY)
        self.sheets_service = self._authenticate_google_sheets()
        self.existing_channels = self._load_existing_channel_ids(spreadsheet_id, sheet_name)
        
    def _authenticate_google_sheets(self):
        """Google Sheets APIの認証（環境変数からサービスアカウントキーを読み込み）"""
        try:
            # 環境変数からサービスアカウントキーのJSONを取得
            service_account_info = os.getenv('GOOGLE_SERVICE_ACCOUNT_KEY')
            if not service_account_info:
                raise ValueError("GOOGLE_SERVICE_ACCOUNT_KEY環境変数が設定されていません。")
            
            # JSON文字列をパース
            service_account_dict = json.loads(service_account_info)
            
            # サービスアカウント認証情報を作成
            credentials = service_account.Credentials.from_service_account_info(
                service_account_dict, scopes=SCOPES
            )
            
            return build('sheets', 'v4', credentials=credentials)
            
        except json.JSONDecodeError as e:
            logger.error(f"サービスアカウントキーのJSON形式が不正です: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Google Sheets APIの認証に失敗しました: {str(e)}")
            raise

    def _load_existing_channel_ids(self, spreadsheet_id: str, sheet_name: str) -> Set[str]:
        """スプレッドシートから既存のチャンネルIDを読み込む"""
        try:
            range_name = f'{sheet_name}!A2:A'  # A2から最終行まで
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=range_name).execute()
            values = result.get('values', [])
            if not values:
                logger.info("スプレッドシートに既存のチャンネルIDは見つかりませんでした。")
                return set()
            
            existing_ids = {row[0] for row in values if row}
            logger.info(f"{len(existing_ids)}件の既存チャンネルIDをスプレッドシートから読み込みました。")
            return existing_ids
        except Exception as e:
            # シートが存在しない、権限がないなどのエラーをハンドル
            if 'Unable to parse range' in str(e) or 'not found' in str(e):
                logger.warning(f"シート '{sheet_name}' が存在しないか、範囲の指定に問題があります。新規作成として扱います。")
            else:
                logger.error(f"スプレッドシートからのデータ読み込みに失敗しました: {str(e)}")
            return set()

    def _load_category_ids(self) -> List[Dict]:
        """カテゴリIDの設定を読み込み"""
        try:
            with open('config/category_ids.json', 'r', encoding='utf-8') as f:
                return json.load(f)['categories']
        except FileNotFoundError:
            logger.error("config/category_ids.json が見つかりません。")
            return []
        except json.JSONDecodeError:
            logger.error("config/category_ids.json の形式が不正です。")
            return []
    
    def get_popular_videos(self, category_id: str) -> tuple[int, list[str]]:
        """人気動画からチャンネルIDを取得し、総取得数と新規IDリストを返す"""
        try:
            all_channel_ids = set()
            new_channel_ids = set()
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
                
                total_quota += 1
                
                for item in response.get('items', []):
                    channel_id = item['snippet']['channelId']
                    all_channel_ids.add(channel_id)
                    if channel_id not in self.existing_channels:
                        new_channel_ids.add(channel_id)
                
                next_page_token = response.get('nextPageToken')
                
                if not next_page_token or total_quota >= daily_limit:
                    break
                
                time.sleep(1)
            
            fetched_count = len(all_channel_ids)
            logger.info(f"カテゴリID[{category_id}]で{fetched_count}件のユニークチャンネルを発見し、うち{len(new_channel_ids)}件が新規でした。")
            return fetched_count, list(new_channel_ids)
        except Exception as e:
            logger.error(f"動画の取得に失敗しました。カテゴリID[{category_id}]: {str(e)}")
            return 0, []
    
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
    
    def write_to_spreadsheet(self, data: List[Dict], spreadsheet_id: str, sheet_name: str):
        """データをGoogleスプレッドシートに追記する"""
        if not data:
            logger.info("書き込むデータがありません。")
            return

        # 日本語ヘッダーとキーのマッピング
        header_map = {
            'channel_id': 'チャンネルID',
            'title': 'チャンネル名称',
            'description': '説明',
            'email': 'メールアドレス',
            'subscriber_count': '登録者数',
            'view_count': '視聴数',
            'video_count': '動画数',
            'fetched_at': 'チャンネル取得日'
        }

        # 書き込むデータを作成
        values = []
        for d in data:
            row = [d.get(key, '') for key in header_map.keys()]
            values.append(row)

        try:
            # 最終行を取得して、その次の行から追記する
            range_name = f'{sheet_name}!A:A'
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=range_name).execute()
            last_row = len(result.get('values', []))
            
            # ヘッダーが存在しない場合（初回書き込み）はヘッダーを書き込む
            if last_row == 0:
                header_row = [list(header_map.values())]
                self.sheets_service.spreadsheets().values().update(
                    spreadsheetId=spreadsheet_id,
                    range=f'{sheet_name}!A1',
                    valueInputOption='RAW',
                    body={'values': header_row}
                ).execute()
                last_row = 1 # ヘッダー分を考慮

            # データを追記
            body = {
                'values': values
            }
            result = self.sheets_service.spreadsheets().values().append(
                spreadsheetId=spreadsheet_id,
                range=f'{sheet_name}!A{last_row + 1}',
                valueInputOption='RAW',
                body=body,
                insertDataOption='INSERT_ROWS'
            ).execute()
            
            logger.info(f"{result.get('updates', {}).get('updatedCells', 0)} セルが更新されました。")
            logger.info(f"スプレッドシートに {len(values)} 件のデータを追記しました: {spreadsheet_id}")

        except Exception as e:
            logger.error(f"スプレッドシートへの書き込みに失敗しました: {str(e)}")

    def send_slack_notification(self, fetched_count: int, new_channels: List[Dict]):
        """Slackに実行結果を通知"""
        added_count = len(new_channels)
        email_count = sum(1 for c in new_channels if c.get('email') and c['email'] != '取得失敗')
        # 総チャンネル数は、実行前の既存数 + 今回新たに追加された数
        total_count = len(self.existing_channels) + added_count

        # Slackに通知するメッセージを作成
        message = (
            f"🎉 YouTubeチャンネル収集バッチ実行完了！\n\n"
            f"📊 **実行結果**\n"
            f"• 取得チャンネル数: {fetched_count}件\n"
            f"• 新規追加チャンネル数: {added_count}件\n"
            f"• メールアドレス正常抽出数: {email_count}件\n"
            f"• 総チャンネル数: {total_count}件\n"
            f"• 出力先URL: https://docs.google.com/spreadsheets/d/11DqIAdm9ofnr9Zip8YQP2-yqdK4UOf_DTx_eiuJXVmw/edit?gid=0#gid=0\n"
        )

        payload = {"text": message}
        
        try:
            response = requests.post(SLACK_WEBHOOK_URL, json=payload, timeout=10)
            if response.status_code == 200:
                logger.info("Slack通知を送信しました。")
            else:
                logger.error(f"Slack通知の送信に失敗しました。ステータスコード: {response.status_code}")
        except Exception as e:
            logger.error(f"Slack通知の送信中にエラーが発生しました: {str(e)}")

    def run(self, spreadsheet_id: str):
        """メイン処理の実行"""
        logger.info(f"バッチ処理を開始します。")
        
        categories = self._load_category_ids()
        total_fetched_count = 0
        all_new_channels = []
        
        for category in categories:
            logger.info(f"処理中 カテゴリ: {category['name']} (ID: {category['id']})")
            
            fetched_count, new_channel_ids = self.get_popular_videos(category['id'])
            total_fetched_count += fetched_count
            
            if new_channel_ids:
                channels: List[Dict] = self.get_channel_details(new_channel_ids)
                all_new_channels.extend(channels)
                
                logger.info(f"{len(channels)} new channels added from category {category['name']}")
            
            time.sleep(1)
        
        logger.info(f"処理が完了しました。合計取得チャンネル数: {total_fetched_count}, 新規追加対象: {len(all_new_channels)}")
        
        if all_new_channels:
            sheet_name = os.getenv('SHEET_NAME', 'Sheet1')
            logger.info(f"シート '{sheet_name}' にデータを書き込みます")
            self.write_to_spreadsheet(all_new_channels, spreadsheet_id, sheet_name)
        else:
            logger.info("新規チャンネルが取得されなかったため、スプレッドシートへの書き込みはスキップされました。")

        logger.info(f"Slack通知処理を開始します。")
        self.send_slack_notification(total_fetched_count, all_new_channels)

if __name__ == '__main__':

    # 環境変数の読み込み
    load_dotenv()
    YOUTUBE_API_KEY = os.getenv('YOUTUBE_API_KEY')
    MIN_SUBSCRIBER_COUNT = int(os.getenv('MIN_SUBSCRIBER_COUNT', '100000'))  # 10万未満除外
    SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL')

    if not YOUTUBE_API_KEY:
        raise ValueError("YOUTUBE_API_KEYが設定されていません。")

    # スプレッドシートIDを環境変数から取得
    SPREADSHEET_ID = os.getenv('SPREADSHEET_ID')
    if not SPREADSHEET_ID:
        raise ValueError("SPREADSHEET_IDが設定されていません。")

    # サービスアカウントキーの環境変数チェック
    GOOGLE_SERVICE_ACCOUNT_KEY = os.getenv('GOOGLE_SERVICE_ACCOUNT_KEY')
    if not GOOGLE_SERVICE_ACCOUNT_KEY:
        raise ValueError("GOOGLE_SERVICE_ACCOUNT_KEY環境変数が設定されていません。")

    if not SLACK_WEBHOOK_URL:
        raise ValueError("SLACK_WEBHOOK_URLが設定されていません。")

    # シート名を環境変数から取得（デフォルトは'Sheet1'）
    SHEET_NAME = os.getenv('SHEET_NAME', 'Sheet1')

    collector = YouTubeChannelCollector(SPREADSHEET_ID, SHEET_NAME)
    collector.run(SPREADSHEET_ID)
