#!/usr/bin/env python3
"""
緯度経度付き新規データベース作成ETL
既存のprops.dbから住所データを読み込み、jageocoderでジオコーディングして新しいDBに出力
"""

import sqlite3
import pandas as pd
from tqdm import tqdm
import jageocoder
import logging
import os
from datetime import datetime
from typing import Tuple, Optional

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class GeocodedDatabaseCreator:
    def __init__(self, source_db_path: str, output_db_path: str):
        """
        新規ジオコーディングDB作成クラス

        Args:
            source_db_path: 元データベースのパス
            output_db_path: 出力先データベースのパス
        """
        self.source_db_path = source_db_path
        self.output_db_path = output_db_path
        self.source_conn = None
        self.output_conn = None

    def connect_databases(self):
        """データベースに接続"""
        try:
            self.source_conn = sqlite3.connect(self.source_db_path)
            self.output_conn = sqlite3.connect(self.output_db_path)
            logger.info(f"ソースDB接続: {self.source_db_path}")
            logger.info(f"出力DB作成: {self.output_db_path}")
        except Exception as e:
            logger.error(f"データベース接続エラー: {e}")
            raise

    def close_databases(self):
        """データベース接続を閉じる"""
        if self.source_conn:
            self.source_conn.close()
            logger.info("ソースDB接続を閉じました")
        if self.output_conn:
            self.output_conn.close()
            logger.info("出力DB接続を閉じました")

    def create_output_table(self):
        """出力テーブルを作成（元テーブル構造 + 緯度経度カラム）"""
        cursor_source = self.source_conn.cursor()
        cursor_output = self.output_conn.cursor()

        # 元テーブルの構造を取得
        cursor_source.execute("PRAGMA table_info(BUY_data_url_uniqued)")
        columns = cursor_source.fetchall()

        # CREATE TABLE文を構築
        column_definitions = []
        existing_columns = set()

        for col in columns:
            column_name = col[1].lower()
            existing_columns.add(column_name)
            column_definitions.append(f"{col[1]} {col[2]}")

        # 緯度経度カラムが存在しない場合のみ追加
        if 'latitude' not in existing_columns:
            column_definitions.append("latitude REAL")
        if 'longitude' not in existing_columns:
            column_definitions.append("longitude REAL")
        if 'geocoded_at' not in existing_columns:
            column_definitions.append("geocoded_at TIMESTAMP")

        create_sql = f"""
        CREATE TABLE BUY_data_url_uniqued_with_geocoding (
            {', '.join(column_definitions)}
        )
        """

        cursor_output.execute(create_sql)
        self.output_conn.commit()
        logger.info("出力テーブルを作成しました（緯度経度カラム含む）")

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
                best_result = result['candidates'][0]
                if 'y' in best_result and 'x' in best_result:
                    latitude = float(best_result['y'])
                    longitude = float(best_result['x'])
                    return latitude, longitude

            return None, None

        except Exception as e:
            logger.warning(f"ジオコーディングエラー (住所: {address}): {e}")
            return None, None

    def process_data(self, batch_size: int = 1000, limit: Optional[int] = None):
        """データ処理とジオコーディング実行"""
        cursor_source = self.source_conn.cursor()
        cursor_output = self.output_conn.cursor()

        # 元データの件数を確認
        cursor_source.execute("SELECT COUNT(*) FROM BUY_data_url_uniqued WHERE address IS NOT NULL AND address != ''")
        total_count = cursor_source.fetchone()[0]

        if limit:
            total_count = min(total_count, limit)
            logger.info(f"処理を{limit}件に制限します")

        logger.info(f"処理対象データ: {total_count}件")

        # 元データを取得（バッチ処理）
        limit_clause = f"LIMIT {limit}" if limit else ""
        cursor_source.execute(f"""
        SELECT * FROM BUY_data_url_uniqued
        WHERE address IS NOT NULL AND address != ''
        ORDER BY rowid
        {limit_clause}
        """)

        # カラム名を取得
        column_names = [description[0] for description in cursor_source.description]
        address_col_index = column_names.index('address')

        success_count = 0
        error_count = 0
        batch_data = []

        with tqdm(total=total_count, desc="ジオコーディング処理中") as pbar:
            while True:
                rows = cursor_source.fetchmany(batch_size)
                if not rows:
                    break

                for row in rows:
                    row_data = list(row)
                    address = row_data[address_col_index]

                    # ジオコーディング実行
                    latitude, longitude = self.geocode_address(address)

                    if latitude is not None and longitude is not None:
                        success_count += 1
                    else:
                        error_count += 1

                    # 既存カラムの緯度経度を更新、またはgeocode_atを追加
                    if 'latitude' in [col.lower() for col in column_names]:
                        # 既存の緯度経度カラムがある場合は更新
                        lat_index = next(i for i, col in enumerate(column_names) if col.lower() == 'latitude')
                        lon_index = next(i for i, col in enumerate(column_names) if col.lower() == 'longitude')
                        row_data[lat_index] = latitude
                        row_data[lon_index] = longitude
                        # geocoded_atがあれば追加
                        if 'geocoded_at' not in [col.lower() for col in column_names]:
                            row_data.append(datetime.now().isoformat())
                    else:
                        # 新規カラムとして追加
                        row_data.extend([
                            latitude,
                            longitude,
                            datetime.now().isoformat()
                        ])

                    batch_data.append(tuple(row_data))
                    pbar.update(1)
                    pbar.set_postfix({
                        'success': success_count,
                        'error': error_count
                    })

                # バッチ挿入
                if batch_data:
                    # 実際のデータ長に基づいてプレースホルダーを決定
                    data_length = len(batch_data[0]) if batch_data else len(column_names)
                    placeholders = ', '.join(['?'] * data_length)
                    insert_sql = f"INSERT INTO BUY_data_url_uniqued_with_geocoding VALUES ({placeholders})"
                    cursor_output.executemany(insert_sql, batch_data)
                    self.output_conn.commit()
                    batch_data = []

        logger.info(f"ジオコーディング処理完了 - 成功: {success_count}件, エラー: {error_count}件")
        return success_count, error_count

    def create_indexes(self):
        """インデックスを作成して検索を高速化"""
        cursor = self.output_conn.cursor()

        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_address ON BUY_data_url_uniqued_with_geocoding(address)",
            "CREATE INDEX IF NOT EXISTS idx_latitude ON BUY_data_url_uniqued_with_geocoding(latitude)",
            "CREATE INDEX IF NOT EXISTS idx_longitude ON BUY_data_url_uniqued_with_geocoding(longitude)",
            "CREATE INDEX IF NOT EXISTS idx_pref ON BUY_data_url_uniqued_with_geocoding(pref)",
            "CREATE INDEX IF NOT EXISTS idx_geocoded_at ON BUY_data_url_uniqued_with_geocoding(geocoded_at)"
        ]

        for index_sql in indexes:
            cursor.execute(index_sql)

        self.output_conn.commit()
        logger.info("検索用インデックスを作成しました")

    def get_output_statistics(self):
        """出力データベースの統計情報を表示"""
        cursor = self.output_conn.cursor()

        # 全レコード数
        cursor.execute("SELECT COUNT(*) FROM BUY_data_url_uniqued_with_geocoding")
        total_count = cursor.fetchone()[0]

        # ジオコーディング成功数
        cursor.execute("""
        SELECT COUNT(*) FROM BUY_data_url_uniqued_with_geocoding
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """)
        geocoded_count = cursor.fetchone()[0]

        # ファイルサイズ
        file_size = os.path.getsize(self.output_db_path)
        file_size_mb = file_size / (1024 * 1024)

        completion_rate = (geocoded_count / total_count * 100) if total_count > 0 else 0

        logger.info("=" * 50)
        logger.info("出力データベース統計:")
        logger.info(f"  ファイルパス: {self.output_db_path}")
        logger.info(f"  ファイルサイズ: {file_size_mb:.1f}MB")
        logger.info(f"  全レコード数: {total_count:,}件")
        logger.info(f"  ジオコーディング成功: {geocoded_count:,}件")
        logger.info(f"  成功率: {completion_rate:.1f}%")
        logger.info("=" * 50)

    def show_sample_results(self, limit: int = 5):
        """サンプル結果を表示"""
        cursor = self.output_conn.cursor()
        cursor.execute("""
        SELECT address, latitude, longitude, geocoded_at
        FROM BUY_data_url_uniqued_with_geocoding
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY geocoded_at DESC
        LIMIT ?
        """, (limit,))

        results = cursor.fetchall()
        logger.info(f"最新ジオコーディング結果（{limit}件）:")
        for address, lat, lon, geocoded_at in results:
            logger.info(f"  {address} → ({lat:.6f}, {lon:.6f}) [{geocoded_at}]")

    def run_etl(self, batch_size: int = 1000, limit: Optional[int] = None):
        """ETL処理をフル実行"""
        try:
            logger.info("=" * 60)
            logger.info("緯度経度付きデータベース作成ETLを開始します")
            logger.info("=" * 60)

            # jageocoderの初期化
            logger.info("jageocoderを初期化中...")
            jageocoder.init()
            logger.info("jageocoder初期化完了")

            # データベース接続
            self.connect_databases()

            # 出力テーブル作成
            self.create_output_table()

            # データ処理・ジオコーディング実行
            success_count, error_count = self.process_data(batch_size=batch_size, limit=limit)

            # インデックス作成
            self.create_indexes()

            # 統計情報表示
            self.get_output_statistics()

            # サンプル結果表示
            self.show_sample_results()

            logger.info("=" * 60)
            logger.info("ETL処理が正常に完了しました")
            logger.info("=" * 60)

            return success_count, error_count

        except Exception as e:
            logger.error(f"ETL処理中にエラーが発生しました: {e}")
            raise
        finally:
            self.close_databases()


def main():
    """メイン処理"""
    # パス設定
    source_db_path = "/Users/tsukasa/Arealinks/Apps4/backend/data/props.db"

    # タイムスタンプ付きの出力ファイル名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_db_path = f"/Users/tsukasa/Arealinks/db_ETL/props_with_geocoding_{timestamp}.db"

    # 出力予定を表示
    logger.info(f"ソースDB: {source_db_path}")
    logger.info(f"出力DB: {output_db_path}")

    # ETL実行（デフォルト: 全件処理、テスト用にlimit=1000など指定可能）
    creator = GeocodedDatabaseCreator(source_db_path, output_db_path)

    # 実行モード選択
    print("実行モード選択:")
    print("1. テスト実行 (100件)")
    print("2. 中規模実行 (10,000件)")
    print("3. 全件実行 (640,000件以上)")

    choice = input("選択してください [1-3]: ").strip()

    if choice == "1":
        limit = 100
        print("テスト実行モード: 100件を処理します")
    elif choice == "2":
        limit = 10000
        print("中規模実行モード: 10,000件を処理します")
    elif choice == "3":
        limit = None
        print("全件実行モード: 全てのデータを処理します")
        try:
            confirm = input("全件処理には時間がかかります。続行しますか？ [y/N]: ").strip().lower()
            if confirm != 'y':
                print("処理を中止しました")
                return
        except EOFError:
            # 非対話モードの場合は自動で続行
            print("非対話モードで全件処理を開始します")
            pass
    else:
        print("無効な選択です。テスト実行モードで実行します")
        limit = 100

    creator.run_etl(batch_size=1000, limit=limit)


if __name__ == "__main__":
    main()