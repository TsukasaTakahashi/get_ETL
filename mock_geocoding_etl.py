#!/usr/bin/env python3
"""
住所ジオコーディング ETL (模擬版)
実際のjageocoder辞書の設定が完了するまでの間、模擬的な緯度経度を生成してETLの動作を確認する
"""

import sqlite3
import pandas as pd
from tqdm import tqdm
import logging
import hashlib
import re
from typing import Tuple, Optional

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MockAddressGeocodingETL:
    def __init__(self, db_path: str):
        """
        ETLクラスの初期化

        Args:
            db_path: データベースファイルのパス
        """
        self.db_path = db_path
        self.connection = None

        # 都道府県の大まかな座標範囲（模擬データ用）
        self.prefecture_coords = {
            '北海道': (43.0, 141.3),
            '青森県': (40.8, 140.7),
            '岩手県': (39.7, 141.2),
            '宮城県': (38.3, 140.9),
            '秋田県': (39.7, 140.1),
            '山形県': (38.2, 140.4),
            '福島県': (37.8, 140.5),
            '茨城県': (36.3, 140.4),
            '栃木県': (36.6, 139.9),
            '群馬県': (36.4, 139.1),
            '埼玉県': (35.9, 139.6),
            '千葉県': (35.6, 140.1),
            '東京都': (35.7, 139.7),
            '神奈川県': (35.4, 139.4),
            '新潟県': (37.9, 139.0),
            '富山県': (36.7, 137.2),
            '石川県': (36.6, 136.6),
            '福井県': (36.1, 136.2),
            '山梨県': (35.7, 138.6),
            '長野県': (36.7, 138.2),
            '岐阜県': (35.4, 137.0),
            '静岡県': (34.9, 138.4),
            '愛知県': (35.2, 137.0),
            '三重県': (34.7, 136.5),
            '滋賀県': (35.0, 136.0),
            '京都府': (35.0, 135.8),
            '大阪府': (34.7, 135.5),
            '兵庫県': (34.7, 135.2),
            '奈良県': (34.7, 135.8),
            '和歌山県': (34.2, 135.2),
            '鳥取県': (35.5, 134.2),
            '島根県': (35.5, 133.1),
            '岡山県': (34.7, 133.9),
            '広島県': (34.4, 132.5),
            '山口県': (34.2, 131.5),
            '徳島県': (34.1, 134.6),
            '香川県': (34.3, 134.0),
            '愛媛県': (33.8, 132.8),
            '高知県': (33.6, 133.5),
            '福岡県': (33.6, 130.4),
            '佐賀県': (33.3, 130.3),
            '長崎県': (32.7, 129.9),
            '熊本県': (32.8, 130.7),
            '大分県': (33.2, 131.6),
            '宮崎県': (31.9, 131.4),
            '鹿児島県': (31.6, 130.6),
            '沖縄県': (26.2, 127.7)
        }

    def connect_db(self):
        """データベースに接続"""
        try:
            self.connection = sqlite3.connect(self.db_path)
            logger.info(f"データベースに接続しました: {self.db_path}")
        except Exception as e:
            logger.error(f"データベース接続エラー: {e}")
            raise

    def close_db(self):
        """データベース接続を閉じる"""
        if self.connection:
            self.connection.close()
            logger.info("データベース接続を閉じました")

    def add_geocoding_columns(self):
        """緯度経度カラムを追加"""
        cursor = self.connection.cursor()

        try:
            cursor.execute("ALTER TABLE BUY_data_url_uniqued ADD COLUMN latitude REAL")
            logger.info("latitudeカラムを追加しました")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                logger.info("latitudeカラムは既に存在します")
            else:
                raise

        try:
            cursor.execute("ALTER TABLE BUY_data_url_uniqued ADD COLUMN longitude REAL")
            logger.info("longitudeカラムを追加しました")
        except sqlite3.OperationalError as e:
            if "duplicate column name" in str(e):
                logger.info("longitudeカラムは既に存在します")
            else:
                raise

        self.connection.commit()

    def extract_prefecture(self, address: str) -> Optional[str]:
        """住所から都道府県を抽出"""
        if not address:
            return None

        for prefecture in self.prefecture_coords.keys():
            if address.startswith(prefecture):
                return prefecture

        return None

    def mock_geocode_address(self, address: str) -> Tuple[Optional[float], Optional[float]]:
        """
        住所から緯度経度を模擬生成

        Args:
            address: 住所文字列

        Returns:
            (緯度, 経度) のタプル。失敗した場合は (None, None)
        """
        try:
            if not address or address.strip() == '':
                return None, None

            # 都道府県を抽出
            prefecture = self.extract_prefecture(address)
            if not prefecture:
                return None, None

            # ベース座標を取得
            base_lat, base_lon = self.prefecture_coords[prefecture]

            # 住所文字列をハッシュ化してランダムな偏差を生成
            address_hash = hashlib.md5(address.encode('utf-8')).hexdigest()
            hash_int = int(address_hash[:8], 16)

            # 偏差を計算（±0.5度程度の範囲）
            lat_offset = (hash_int % 1000) / 1000.0 - 0.5
            lon_offset = ((hash_int >> 10) % 1000) / 1000.0 - 0.5

            # 最終座標を計算
            latitude = round(base_lat + lat_offset, 6)
            longitude = round(base_lon + lon_offset, 6)

            return latitude, longitude

        except Exception as e:
            logger.warning(f"模擬ジオコーディングエラー (住所: {address}): {e}")
            return None, None

    def load_addresses(self) -> pd.DataFrame:
        """住所データを読み込み"""
        query = """
        SELECT id, address, latitude, longitude
        FROM BUY_data_url_uniqued
        WHERE address IS NOT NULL
        AND address != ''
        ORDER BY id
        """

        df = pd.read_sql_query(query, self.connection)
        logger.info(f"住所データを読み込みました: {len(df)}件")

        return df

    def filter_unprocessed_addresses(self, df: pd.DataFrame) -> pd.DataFrame:
        """未処理の住所データをフィルタリング"""
        # 緯度経度がNullまたは空の行のみ処理対象
        unprocessed = df[
            (df['latitude'].isna()) |
            (df['longitude'].isna()) |
            (df['latitude'] == '') |
            (df['longitude'] == '')
        ]

        logger.info(f"未処理の住所データ: {len(unprocessed)}件")
        return unprocessed

    def process_geocoding(self, batch_size: int = 100, limit: Optional[int] = None):
        """ジオコーディング処理を実行"""
        # 住所データを読み込み
        df = self.load_addresses()

        if df.empty:
            logger.info("処理対象の住所データがありません")
            return

        # 未処理データをフィルタリング
        unprocessed_df = self.filter_unprocessed_addresses(df)

        if unprocessed_df.empty:
            logger.info("すべての住所データは既に処理済みです")
            return

        # limitが指定されている場合は先頭N件のみ処理
        if limit:
            unprocessed_df = unprocessed_df.head(limit)
            logger.info(f"処理を{limit}件に制限しました")

        logger.info(f"模擬ジオコーディング処理を開始します: {len(unprocessed_df)}件")

        success_count = 0
        error_count = 0

        # バッチ処理でジオコーディング実行
        with tqdm(total=len(unprocessed_df), desc="模擬ジオコーディング実行中") as pbar:
            batch_updates = []

            for idx, row in unprocessed_df.iterrows():
                address = row['address']
                row_id = row['id']

                # 模擬ジオコーディング実行
                latitude, longitude = self.mock_geocode_address(address)

                if latitude is not None and longitude is not None:
                    batch_updates.append((latitude, longitude, row_id))
                    success_count += 1
                else:
                    error_count += 1

                # バッチサイズに達したらデータベースを更新
                if len(batch_updates) >= batch_size:
                    self._execute_batch_update(batch_updates)
                    batch_updates = []

                pbar.update(1)
                pbar.set_postfix({
                    'success': success_count,
                    'error': error_count
                })

            # 残りのバッチを処理
            if batch_updates:
                self._execute_batch_update(batch_updates)

        logger.info(f"模擬ジオコーディング処理完了 - 成功: {success_count}件, エラー: {error_count}件")

    def _execute_batch_update(self, batch_updates):
        """バッチ更新を実行"""
        cursor = self.connection.cursor()
        cursor.executemany(
            "UPDATE BUY_data_url_uniqued SET latitude = ?, longitude = ? WHERE id = ?",
            batch_updates
        )
        self.connection.commit()

    def get_statistics(self):
        """処理統計を取得"""
        cursor = self.connection.cursor()

        # 全レコード数
        cursor.execute("SELECT COUNT(*) FROM BUY_data_url_uniqued WHERE address IS NOT NULL AND address != ''")
        total_count = cursor.fetchone()[0]

        # ジオコーディング済みレコード数
        cursor.execute("""
        SELECT COUNT(*) FROM BUY_data_url_uniqued
        WHERE address IS NOT NULL AND address != ''
        AND latitude IS NOT NULL AND longitude IS NOT NULL
        """)
        geocoded_count = cursor.fetchone()[0]

        completion_rate = (geocoded_count / total_count * 100) if total_count > 0 else 0

        logger.info(f"処理統計:")
        logger.info(f"  全住所レコード数: {total_count}件")
        logger.info(f"  ジオコーディング済み: {geocoded_count}件")
        logger.info(f"  完了率: {completion_rate:.1f}%")

        return {
            'total_count': total_count,
            'geocoded_count': geocoded_count,
            'completion_rate': completion_rate
        }

    def show_sample_results(self, limit: int = 5):
        """サンプル結果を表示"""
        cursor = self.connection.cursor()
        cursor.execute("""
        SELECT address, latitude, longitude
        FROM BUY_data_url_uniqued
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        LIMIT ?
        """, (limit,))

        results = cursor.fetchall()
        logger.info("サンプル結果:")
        for address, lat, lon in results:
            logger.info(f"  {address} -> ({lat}, {lon})")

    def run_etl(self, limit: Optional[int] = 10):
        """ETL処理をフル実行"""
        try:
            logger.info("住所ジオコーディングETL（模擬版）を開始します")

            # データベース接続
            self.connect_db()

            # カラム追加（必要に応じて）
            self.add_geocoding_columns()

            # ジオコーディング処理実行
            self.process_geocoding(limit=limit)

            # 統計情報表示
            self.get_statistics()

            # サンプル結果表示
            self.show_sample_results()

            logger.info("ETL処理が正常に完了しました")

        except Exception as e:
            logger.error(f"ETL処理中にエラーが発生しました: {e}")
            raise
        finally:
            self.close_db()


def main():
    """メイン処理"""
    # データベースパス
    db_path = "/Users/tsukasa/Arealinks/Apps6/sumai_agent6/backend/data/props.db"

    # ETL実行（テスト用に10件のみ処理）
    etl = MockAddressGeocodingETL(db_path)
    etl.run_etl(limit=10)


if __name__ == "__main__":
    main()