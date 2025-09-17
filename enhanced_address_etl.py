#!/usr/bin/env python3
"""
Enhanced Address Processing ETL
住所データの構造化・正規化・空間グリッド・階層情報を追加する拡張ETL
"""

import sqlite3
import pandas as pd
from tqdm import tqdm
import jageocoder
import logging
import os
import re
import hashlib
import json
from datetime import datetime
from typing import Tuple, Optional, Dict, List, Any
import unicodedata
import jaconv

# ログ設定
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class EnhancedAddressETL:
    def __init__(self, source_db_path: str, output_db_path: str):
        """
        拡張住所処理ETLクラス

        Args:
            source_db_path: 元データベースのパス
            output_db_path: 出力先データベースのパス
        """
        self.source_db_path = source_db_path
        self.output_db_path = output_db_path
        self.source_conn = None
        self.output_conn = None

        # JISコードマッピング（サンプル - 実際は外部データを参照）
        self.pref_codes = {
            '北海道': '01', '青森県': '02', '岩手県': '03', '宮城県': '04', '秋田県': '05',
            '山形県': '06', '福島県': '07', '茨城県': '08', '栃木県': '09', '群馬県': '10',
            '埼玉県': '11', '千葉県': '12', '東京都': '13', '神奈川県': '14', '新潟県': '15',
            '富山県': '16', '石川県': '17', '福井県': '18', '山梨県': '19', '長野県': '20',
            '岐阜県': '21', '静岡県': '22', '愛知県': '23', '三重県': '24', '滋賀県': '25',
            '京都府': '26', '大阪府': '27', '兵庫県': '28', '奈良県': '29', '和歌山県': '30',
            '鳥取県': '31', '島根県': '32', '岡山県': '33', '広島県': '34', '山口県': '35',
            '徳島県': '36', '香川県': '37', '愛媛県': '38', '高知県': '39', '福岡県': '40',
            '佐賀県': '41', '長崎県': '42', '熊本県': '43', '大分県': '44', '宮崎県': '45',
            '鹿児島県': '46', '沖縄県': '47'
        }

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

    def create_enhanced_table(self):
        """拡張テーブルを作成"""
        cursor_source = self.source_conn.cursor()
        cursor_output = self.output_conn.cursor()

        # 元テーブルの構造を取得
        cursor_source.execute("PRAGMA table_info(BUY_data_url_uniqued)")
        columns = cursor_source.fetchall()

        # 元のカラムリストを作成
        original_columns = []
        for col in columns:
            original_columns.append(f"{col[1]} {col[2]}")

        # 拡張カラムを定義
        enhanced_columns = [
            # A. 行政コード/ID（機械可読）
            "pref_code TEXT",
            "municipality_code TEXT",
            "ward_code TEXT",
            "town_code TEXT",

            # B. 住所の構造化（人間可読）
            "municipality_name TEXT",
            "city_name TEXT",
            "ward_name TEXT",
            "town_name TEXT",
            "chome_num INTEGER",
            "ban TEXT",
            "go TEXT",
            "building_name TEXT",
            "room_no TEXT",

            # C. 正規化キー
            "addr_std TEXT",
            "addr_key TEXT",
            "pref_kana TEXT",
            "municipality_kana TEXT",
            "town_kana TEXT",
            "addr_roman TEXT",

            # D. 緯度経度（既存）
            "latitude REAL",
            "longitude REAL",
            "geocoded_at TIMESTAMP",

            # E. 空間グリッド
            "geohash_6 TEXT",
            "geohash_8 TEXT",
            "mesh_1km TEXT",
            "mesh_500m TEXT",
            "s2_cell_l12 TEXT",

            # F. ヒエラルキー/クラスタ/品質
            "admin_path TEXT",  # JSON文字列
            "admin_ids TEXT",   # JSON文字列
            "postal_code TEXT",
            "region_block TEXT",
            "address_parse_score REAL",
            "normalize_warnings TEXT"  # JSON文字列
        ]

        # CREATE TABLE文を構築
        all_columns = original_columns + enhanced_columns
        create_sql = f"""
        CREATE TABLE BUY_data_enhanced (
            {', '.join(all_columns)}
        )
        """

        cursor_output.execute(create_sql)
        self.output_conn.commit()
        logger.info("拡張テーブルを作成しました")

    def normalize_address(self, address: str) -> Tuple[str, str, List[str]]:
        """
        住所の正規化

        Returns:
            (正規化住所, 検索キー, 警告リスト)
        """
        warnings = []

        if not address or address.strip() == '':
            return '', '', ['空の住所']

        # 全角/半角統一
        normalized = unicodedata.normalize('NFKC', address)

        # 漢数字を算用数字に変換
        try:
            normalized = jaconv.han2zen(normalized, digit=False, ascii=False)
            normalized = jaconv.h2z(normalized, digit=True, ascii=True)
        except:
            warnings.append('数字変換エラー')

        # スペース統一
        normalized = re.sub(r'\s+', ' ', normalized.strip())

        # 検索キー生成（ひらがな化、記号除去、小文字化）
        search_key = normalized
        try:
            search_key = jaconv.kata2hira(search_key)
            search_key = re.sub(r'[^\w\s]', '', search_key)
            search_key = search_key.lower().replace(' ', '')
        except:
            warnings.append('検索キー生成エラー')

        return normalized, search_key, warnings

    def parse_address_components(self, address: str) -> Dict[str, Any]:
        """
        住所を構成要素に分解

        Returns:
            住所構成要素の辞書
        """
        components = {
            'pref_name': None,
            'municipality_name': None,
            'city_name': None,
            'ward_name': None,
            'town_name': None,
            'chome_num': None,
            'ban': None,
            'go': None,
            'building_name': None,
            'room_no': None,
            'parse_score': 0.0,
            'warnings': []
        }

        if not address:
            return components

        # 都道府県の抽出
        pref_match = re.match(r'^(.*?[都道府県])', address)
        if pref_match:
            components['pref_name'] = pref_match.group(1)
            remaining = address[len(pref_match.group(1)):]
            components['parse_score'] += 0.2
        else:
            remaining = address
            components['warnings'].append('都道府県未検出')

        # 市区町村の抽出
        municipality_match = re.match(r'^(.*?[市区町村郡])', remaining)
        if municipality_match:
            components['municipality_name'] = municipality_match.group(1)
            remaining = remaining[len(municipality_match.group(1)):]
            components['parse_score'] += 0.2

            # 市のみを抽出
            if '市' in components['municipality_name']:
                components['city_name'] = components['municipality_name']
        else:
            components['warnings'].append('市区町村未検出')

        # 区の抽出（政令市や特別区）
        ward_match = re.match(r'^(.*?区)', remaining)
        if ward_match:
            components['ward_name'] = ward_match.group(1)
            remaining = remaining[len(ward_match.group(1)):]
            components['parse_score'] += 0.1

        # 大字・町名の抽出
        town_match = re.match(r'^((?:大字|字)?.*?)(\d|[一二三四五六七八九十百千万])', remaining)
        if town_match:
            components['town_name'] = town_match.group(1)
            remaining = remaining[len(town_match.group(1)):]
            components['parse_score'] += 0.2
        else:
            # 番号がない場合は残り全体を町名とする
            if remaining:
                components['town_name'] = remaining
                components['parse_score'] += 0.1
                remaining = ''

        # 丁目番号の抽出
        chome_match = re.search(r'(\d+)丁目', remaining)
        if chome_match:
            components['chome_num'] = int(chome_match.group(1))
            components['parse_score'] += 0.1

        # 番地の抽出
        ban_match = re.search(r'(\d+)番地?', remaining)
        if ban_match:
            components['ban'] = ban_match.group(1)
            components['parse_score'] += 0.1

        # 号の抽出
        go_match = re.search(r'(\d+)号', remaining)
        if go_match:
            components['go'] = go_match.group(1)
            components['parse_score'] += 0.05

        # 建物名の抽出（簡易版）
        building_match = re.search(r'([^\d\s]+(?:マンション|アパート|ビル|ハイツ|コーポ|荘))', remaining)
        if building_match:
            components['building_name'] = building_match.group(1)
            components['parse_score'] += 0.05

        return components

    def calculate_geohash(self, lat: float, lon: float, precision: int) -> str:
        """
        Geohashを計算（簡易版）
        """
        try:
            # 簡易的なgeohash実装
            lat_range = [-90.0, 90.0]
            lon_range = [-180.0, 180.0]

            bits = []
            even = True

            for i in range(precision * 5):  # 各文字は5ビット
                if even:  # 経度
                    mid = (lon_range[0] + lon_range[1]) / 2
                    if lon >= mid:
                        bits.append(1)
                        lon_range[0] = mid
                    else:
                        bits.append(0)
                        lon_range[1] = mid
                else:  # 緯度
                    mid = (lat_range[0] + lat_range[1]) / 2
                    if lat >= mid:
                        bits.append(1)
                        lat_range[0] = mid
                    else:
                        bits.append(0)
                        lat_range[1] = mid
                even = not even

            # ビットを文字列に変換
            base32 = "0123456789bcdefghjkmnpqrstuvwxyz"
            result = ""
            for i in range(0, len(bits), 5):
                chunk = bits[i:i+5]
                while len(chunk) < 5:
                    chunk.append(0)
                value = sum([chunk[j] * (2 ** (4-j)) for j in range(5)])
                result += base32[value]

            return result
        except:
            return None

    def calculate_mesh_code(self, lat: float, lon: float, mesh_size: str) -> str:
        """
        JISメッシュコードを計算
        """
        try:
            # 1次メッシュ（基準地域メッシュ）
            lat_deg = int(lat * 3 / 2)
            lon_deg = int(lon - 100)
            primary = f"{lat_deg:02d}{lon_deg:02d}"

            if mesh_size == "1km":
                # 簡易的な1kmメッシュ（実際はより複雑）
                lat_min = (lat * 3 / 2 - lat_deg) * 8
                lon_min = (lon - 100 - lon_deg) * 8
                return f"{primary}{int(lat_min)}{int(lon_min)}"
            elif mesh_size == "500m":
                # 簡易的な500mメッシュ
                lat_min = (lat * 3 / 2 - lat_deg) * 16
                lon_min = (lon - 100 - lon_deg) * 16
                return f"{primary}{int(lat_min)}{int(lon_min)}"

            return primary
        except:
            return None

    def generate_admin_hierarchy(self, components: Dict[str, Any], pref_code: str) -> Tuple[str, str]:
        """
        行政階層とIDを生成

        Returns:
            (admin_path JSON, admin_ids JSON)
        """
        admin_path = []
        admin_ids = []

        # 都道府県
        if components['pref_name']:
            admin_path.append({
                'level': 'prefecture',
                'id': pref_code,
                'name': components['pref_name']
            })
            admin_ids.append(f"pref:{pref_code}")

        # 市区町村
        if components['municipality_name']:
            # 簡易的な市区町村コード生成
            muni_code = f"{pref_code}000"  # 実際は正確な対応表が必要
            admin_path.append({
                'level': 'municipality',
                'id': muni_code,
                'name': components['municipality_name']
            })
            admin_ids.append(f"city:{muni_code}")

        # 区（政令市・特別区）
        if components['ward_name']:
            ward_code = f"{pref_code}001"  # 実際は正確な対応表が必要
            admin_path.append({
                'level': 'ward',
                'id': ward_code,
                'name': components['ward_name']
            })
            admin_ids.append(f"ward:{ward_code}")

        # 町・大字
        if components['town_name']:
            town_code = f"{pref_code}0001"  # 実際は正確な対応表が必要
            admin_path.append({
                'level': 'town',
                'id': town_code,
                'name': components['town_name']
            })
            admin_ids.append(f"town:{town_code}")

        return json.dumps(admin_path, ensure_ascii=False), json.dumps(admin_ids, ensure_ascii=False)

    def geocode_address(self, address: str) -> Tuple[Optional[float], Optional[float]]:
        """
        住所から緯度経度を取得（既存の実装を使用）
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

    def process_single_address(self, address: str, pref: str) -> Dict[str, Any]:
        """
        単一住所の包括的処理
        """
        result = {}

        # 住所正規化
        addr_std, addr_key, normalize_warnings = self.normalize_address(address)
        result['addr_std'] = addr_std
        result['addr_key'] = addr_key
        result['normalize_warnings'] = json.dumps(normalize_warnings, ensure_ascii=False)

        # 住所構成要素の解析
        components = self.parse_address_components(address)
        result['municipality_name'] = components['municipality_name']
        result['city_name'] = components['city_name']
        result['ward_name'] = components['ward_name']
        result['town_name'] = components['town_name']
        result['chome_num'] = components['chome_num']
        result['ban'] = components['ban']
        result['go'] = components['go']
        result['building_name'] = components['building_name']
        result['room_no'] = components['room_no']
        result['address_parse_score'] = components['parse_score']

        # 行政コード
        result['pref_code'] = self.pref_codes.get(pref)
        result['municipality_code'] = None  # 実際は詳細な対応表が必要
        result['ward_code'] = None
        result['town_code'] = None

        # 緯度経度取得
        latitude, longitude = self.geocode_address(address)
        result['latitude'] = latitude
        result['longitude'] = longitude
        result['geocoded_at'] = datetime.now().isoformat() if latitude else None

        # 空間グリッド計算
        if latitude and longitude:
            result['geohash_6'] = self.calculate_geohash(latitude, longitude, 6)
            result['geohash_8'] = self.calculate_geohash(latitude, longitude, 8)
            result['mesh_1km'] = self.calculate_mesh_code(latitude, longitude, "1km")
            result['mesh_500m'] = self.calculate_mesh_code(latitude, longitude, "500m")
            result['s2_cell_l12'] = None  # S2実装は複雑なため保留
        else:
            result['geohash_6'] = None
            result['geohash_8'] = None
            result['mesh_1km'] = None
            result['mesh_500m'] = None
            result['s2_cell_l12'] = None

        # 階層情報
        admin_path, admin_ids = self.generate_admin_hierarchy(components, result['pref_code'] or '00')
        result['admin_path'] = admin_path
        result['admin_ids'] = admin_ids

        # その他
        result['postal_code'] = None  # 実際は郵便番号DBとの照合が必要
        result['region_block'] = None
        result['pref_kana'] = None
        result['municipality_kana'] = None
        result['town_kana'] = None
        result['addr_roman'] = None

        return result

    def process_data(self, batch_size: int = 1000, limit: Optional[int] = None):
        """データ処理とEnhanced Address処理実行"""
        cursor_source = self.source_conn.cursor()
        cursor_output = self.output_conn.cursor()

        # 元データの件数を確認
        cursor_source.execute("SELECT COUNT(*) FROM BUY_data_url_uniqued WHERE address IS NOT NULL AND address != ''")
        total_count = cursor_source.fetchone()[0]

        if limit:
            total_count = min(total_count, limit)
            logger.info(f"処理を{limit}件に制限します")

        logger.info(f"処理対象データ: {total_count}件")

        # 元データを取得
        limit_clause = f"LIMIT {limit}" if limit else ""
        cursor_source.execute(f"""
        SELECT * FROM BUY_data_url_uniqued
        WHERE address IS NOT NULL AND address != ''
        ORDER BY rowid
        {limit_clause}
        """)

        # カラム名を取得
        column_names = [description[0] for description in cursor_source.description]

        success_count = 0
        error_count = 0
        batch_data = []

        with tqdm(total=total_count, desc="Enhanced Address処理中") as pbar:
            while True:
                rows = cursor_source.fetchmany(batch_size)
                if not rows:
                    break

                for row in rows:
                    try:
                        row_data = list(row)

                        # 住所とprefを取得
                        address_idx = column_names.index('address')
                        pref_idx = column_names.index('pref')
                        address = row_data[address_idx]
                        pref = row_data[pref_idx]

                        # Enhanced処理実行
                        enhanced_data = self.process_single_address(address, pref)

                        # 拡張データを追加
                        extended_row = row_data + [
                            enhanced_data['pref_code'],
                            enhanced_data['municipality_code'],
                            enhanced_data['ward_code'],
                            enhanced_data['town_code'],
                            enhanced_data['municipality_name'],
                            enhanced_data['city_name'],
                            enhanced_data['ward_name'],
                            enhanced_data['town_name'],
                            enhanced_data['chome_num'],
                            enhanced_data['ban'],
                            enhanced_data['go'],
                            enhanced_data['building_name'],
                            enhanced_data['room_no'],
                            enhanced_data['addr_std'],
                            enhanced_data['addr_key'],
                            enhanced_data['pref_kana'],
                            enhanced_data['municipality_kana'],
                            enhanced_data['town_kana'],
                            enhanced_data['addr_roman'],
                            enhanced_data['latitude'],
                            enhanced_data['longitude'],
                            enhanced_data['geocoded_at'],
                            enhanced_data['geohash_6'],
                            enhanced_data['geohash_8'],
                            enhanced_data['mesh_1km'],
                            enhanced_data['mesh_500m'],
                            enhanced_data['s2_cell_l12'],
                            enhanced_data['admin_path'],
                            enhanced_data['admin_ids'],
                            enhanced_data['postal_code'],
                            enhanced_data['region_block'],
                            enhanced_data['address_parse_score'],
                            enhanced_data['normalize_warnings']
                        ]

                        batch_data.append(tuple(extended_row))
                        success_count += 1

                    except Exception as e:
                        logger.warning(f"処理エラー: {e}")
                        error_count += 1

                    pbar.update(1)
                    pbar.set_postfix({
                        'success': success_count,
                        'error': error_count
                    })

                # バッチ挿入
                if batch_data:
                    placeholders = ', '.join(['?'] * len(batch_data[0]))
                    insert_sql = f"INSERT INTO BUY_data_enhanced VALUES ({placeholders})"
                    cursor_output.executemany(insert_sql, batch_data)
                    self.output_conn.commit()
                    batch_data = []

        logger.info(f"Enhanced Address処理完了 - 成功: {success_count}件, エラー: {error_count}件")
        return success_count, error_count

    def create_indexes(self):
        """検索用インデックスを作成"""
        cursor = self.output_conn.cursor()

        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_address ON BUY_data_enhanced(address)",
            "CREATE INDEX IF NOT EXISTS idx_addr_std ON BUY_data_enhanced(addr_std)",
            "CREATE INDEX IF NOT EXISTS idx_addr_key ON BUY_data_enhanced(addr_key)",
            "CREATE INDEX IF NOT EXISTS idx_latitude ON BUY_data_enhanced(latitude)",
            "CREATE INDEX IF NOT EXISTS idx_longitude ON BUY_data_enhanced(longitude)",
            "CREATE INDEX IF NOT EXISTS idx_pref_code ON BUY_data_enhanced(pref_code)",
            "CREATE INDEX IF NOT EXISTS idx_municipality_code ON BUY_data_enhanced(municipality_code)",
            "CREATE INDEX IF NOT EXISTS idx_geohash_6 ON BUY_data_enhanced(geohash_6)",
            "CREATE INDEX IF NOT EXISTS idx_geohash_8 ON BUY_data_enhanced(geohash_8)",
            "CREATE INDEX IF NOT EXISTS idx_mesh_1km ON BUY_data_enhanced(mesh_1km)",
            "CREATE INDEX IF NOT EXISTS idx_town_name ON BUY_data_enhanced(town_name)",
            "CREATE INDEX IF NOT EXISTS idx_parse_score ON BUY_data_enhanced(address_parse_score)"
        ]

        for index_sql in indexes:
            cursor.execute(index_sql)

        self.output_conn.commit()
        logger.info("Enhanced検索用インデックスを作成しました")

    def get_statistics(self):
        """統計情報を表示"""
        cursor = self.output_conn.cursor()

        # 全レコード数
        cursor.execute("SELECT COUNT(*) FROM BUY_data_enhanced")
        total_count = cursor.fetchone()[0]

        # ジオコーディング成功数
        cursor.execute("""
        SELECT COUNT(*) FROM BUY_data_enhanced
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        """)
        geocoded_count = cursor.fetchone()[0]

        # 住所解析品質統計
        cursor.execute("SELECT AVG(address_parse_score), MIN(address_parse_score), MAX(address_parse_score) FROM BUY_data_enhanced")
        score_stats = cursor.fetchone()

        # ファイルサイズ
        file_size = os.path.getsize(self.output_db_path)
        file_size_mb = file_size / (1024 * 1024)

        completion_rate = (geocoded_count / total_count * 100) if total_count > 0 else 0

        logger.info("=" * 60)
        logger.info("Enhanced Address データベース統計:")
        logger.info(f"  ファイルパス: {self.output_db_path}")
        logger.info(f"  ファイルサイズ: {file_size_mb:.1f}MB")
        logger.info(f"  全レコード数: {total_count:,}件")
        logger.info(f"  ジオコーディング成功: {geocoded_count:,}件")
        logger.info(f"  ジオコーディング成功率: {completion_rate:.1f}%")
        logger.info(f"  住所解析スコア平均: {score_stats[0]:.3f}")
        logger.info(f"  住所解析スコア範囲: {score_stats[1]:.3f} - {score_stats[2]:.3f}")
        logger.info("=" * 60)

    def show_sample_results(self, limit: int = 3):
        """サンプル結果を表示"""
        cursor = self.output_conn.cursor()
        cursor.execute("""
        SELECT address, addr_std, addr_key, latitude, longitude,
               geohash_6, mesh_1km, admin_path, address_parse_score
        FROM BUY_data_enhanced
        WHERE latitude IS NOT NULL AND longitude IS NOT NULL
        ORDER BY address_parse_score DESC
        LIMIT ?
        """, (limit,))

        results = cursor.fetchall()
        logger.info(f"Enhanced Address処理結果サンプル（{limit}件）:")
        for row in results:
            address, addr_std, addr_key, lat, lon, geohash, mesh, admin_path, score = row
            logger.info(f"  元住所: {address}")
            logger.info(f"  正規化: {addr_std}")
            logger.info(f"  検索キー: {addr_key}")
            logger.info(f"  座標: ({lat:.6f}, {lon:.6f})")
            logger.info(f"  GeoHash: {geohash}")
            logger.info(f"  メッシュ: {mesh}")
            logger.info(f"  解析スコア: {score:.3f}")
            logger.info(f"  階層: {admin_path}")
            logger.info("-" * 50)

    def run_etl(self, batch_size: int = 1000, limit: Optional[int] = None):
        """Enhanced Address ETL処理をフル実行"""
        try:
            logger.info("=" * 70)
            logger.info("Enhanced Address Processing ETLを開始します")
            logger.info("=" * 70)

            # jageocoderの初期化
            logger.info("jageocoderを初期化中...")
            jageocoder.init()
            logger.info("jageocoder初期化完了")

            # データベース接続
            self.connect_databases()

            # 拡張テーブル作成
            self.create_enhanced_table()

            # Enhanced Address処理実行
            success_count, error_count = self.process_data(batch_size=batch_size, limit=limit)

            # インデックス作成
            self.create_indexes()

            # 統計情報表示
            self.get_statistics()

            # サンプル結果表示
            self.show_sample_results()

            logger.info("=" * 70)
            logger.info("Enhanced Address ETL処理が正常に完了しました")
            logger.info("=" * 70)

            return success_count, error_count

        except Exception as e:
            logger.error(f"Enhanced Address ETL処理中にエラーが発生しました: {e}")
            raise
        finally:
            self.close_databases()


def main():
    """メイン処理"""
    # パス設定
    source_db_path = "/Users/tsukasa/Arealinks/Apps4/backend/data/props.db"

    # タイムスタンプ付きの出力ファイル名
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_db_path = f"/Users/tsukasa/Arealinks/db_ETL/props_enhanced_{timestamp}.db"

    # 出力予定を表示
    logger.info(f"ソースDB: {source_db_path}")
    logger.info(f"出力DB: {output_db_path}")

    # ETL実行
    etl = EnhancedAddressETL(source_db_path, output_db_path)

    # 実行モード選択
    print("Enhanced Address ETL実行モード選択:")
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
            print("非対話モードで全件処理を開始します")
            pass
    else:
        print("無効な選択です。テスト実行モードで実行します")
        limit = 100

    etl.run_etl(batch_size=1000, limit=limit)


if __name__ == "__main__":
    main()