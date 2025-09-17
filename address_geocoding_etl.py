#!/usr/bin/env python3
"""
住所ジオコーディング ETL
jageocoderを使用して住所データから緯度経度を取得し、データベースに追加する
"""

import sqlite3
import pandas as pd
from tqdm import tqdm
import jageocoder
import logging
from typing import Tuple, Optional

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class AddressGeocodingETL:
    def __init__(self, db_path: str):
        """
        ETLクラスの初期化

        Args:
            db_path: データベースファイルのパス
        """
        self.db_path = db_path
        self.connection = None

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

    def check_columns_exist(self) -> bool:
        """緯度経度カラムが既に存在するかチェック"""
        cursor = self.connection.cursor()
        cursor.execute("PRAGMA table_info(BUY_data_url_uniqued)")
        columns = [column[1] for column in cursor.fetchall()]

        has_latitude = 'latitude' in columns
        has_longitude = 'longitude' in columns

        logger.info(f"既存カラム確認 - latitude: {has_latitude}, longitude: {has_longitude}")
        return has_latitude and has_longitude

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

    def geocode_address(self, address: str) -> Tuple[Optional[float], Optional[float]]:
        """
        住所から緯度経度を取得

        Args:
            address: 住所文字列

        Returns:
            (緯度, 経度) のタプル。失敗した場合は (None, None)
        """
        try:
            if not address or address.strip() == '':
                return None, None

            result = jageocoder.search(address)
            if result and 'candidates' in result and len(result['candidates']) > 0:
                # 最も確度の高い結果を使用
                best_result = result['candidates'][0]
                if 'y' in best_result and 'x' in best_result:
                    latitude = float(best_result['y'])
                    longitude = float(best_result['x'])
                    return latitude, longitude

            return None, None

        except Exception as e:
            logger.warning(f"ジオコーディングエラー (住所: {address}): {e}")
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

    def update_coordinates(self, row_id: int, latitude: float, longitude: float):
        """データベースの緯度経度を更新"""
        cursor = self.connection.cursor()
        cursor.execute(
            "UPDATE BUY_data_url_uniqued SET latitude = ?, longitude = ? WHERE id = ?",
            (latitude, longitude, row_id)
        )

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

        logger.info(f"ジオコーディング処理を開始します: {len(unprocessed_df)}件")

        success_count = 0
        error_count = 0

        # バッチ処理でジオコーディング実行
        with tqdm(total=len(unprocessed_df), desc="ジオコーディング実行中") as pbar:
            batch_updates = []

            for idx, row in unprocessed_df.iterrows():
                address = row['address']
                row_id = row['id']

                # ジオコーディング実行
                latitude, longitude = self.geocode_address(address)

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

        logger.info(f"ジオコーディング処理完了 - 成功: {success_count}件, エラー: {error_count}件")

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

    def run_etl(self, limit: Optional[int] = 5):
        """ETL処理をフル実行"""
        try:
            logger.info("住所ジオコーディングETLを開始します")

            # jageocoderの初期化
            logger.info("jageocoderを初期化中...")
            jageocoder.init()
            logger.info("jageocoder初期化完了")

            # データベース接続
            self.connect_db()

            # カラム追加（必要に応じて）
            self.add_geocoding_columns()

            # ジオコーディング処理実行（テスト用に制限）
            self.process_geocoding(limit=limit)

            # 統計情報表示
            self.get_statistics()

            logger.info("ETL処理が正常に完了しました")

        except Exception as e:
            logger.error(f"ETL処理中にエラーが発生しました: {e}")
            raise
        finally:
            self.close_db()


def main():
    """メイン処理"""
    # データベースパス（実際に存在するファイルを指定）
    db_path = "/Users/tsukasa/Arealinks/Apps4/backend/data/props.db"

    # ETL実行（テスト用に5件のみ処理）
    etl = AddressGeocodingETL(db_path)
    etl.run_etl(limit=5)


if __name__ == "__main__":
    main()