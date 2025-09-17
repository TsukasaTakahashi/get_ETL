# Enhanced Address Processing ETL

住所データの構造化・正規化・空間グリッド・階層情報を追加する拡張ETLシステム

## 概要

このETLシステムは、既存の住所データに対して以下の機能を提供します：

1. **基本ジオコーディング**: 住所から緯度経度を取得
2. **拡張住所処理**: 住所の構造化、正規化、空間グリッド計算、階層情報生成

## ファイル構成

```
db_ETL/
├── create_geocoded_db.py          # 基本ジオコーディングETL（新規DB作成）
├── address_geocoding_etl.py       # 基本ジオコーディングETL（既存DB更新）
├── enhanced_address_etl.py        # 拡張住所処理ETL
├── integrated_geocoding_etl.py    # 統合ETL（基本+拡張選択可能）
├── mock_geocoding_etl.py          # モック実装（テスト用）
├── test_jageocoder.py            # jageocoderテストスクリプト
├── requirements.txt              # 依存関係
└── README_enhanced.md            # このファイル
```

## 拡張機能の詳細

### A. 行政コード/ID（機械可読）

| カラム名 | 型 | 説明 |
|----------|-----|------|
| pref_code | TEXT | JIS X0401都道府県コード |
| municipality_code | TEXT | JIS X0402市区町村コード |
| ward_code | TEXT | 政令市区/特別区コード |
| town_code | TEXT | 大字・町丁目ID |

### B. 住所の構造化（人間可読）

| カラム名 | 型 | 説明 |
|----------|-----|------|
| municipality_name | TEXT | 市区町村名 |
| city_name | TEXT | 市名のみ（「◯◯市」） |
| ward_name | TEXT | 区名のみ（「◯◯区」） |
| town_name | TEXT | 大字・町名 |
| chome_num | INTEGER | 丁番号（整数） |
| ban | TEXT | 番地 |
| go | TEXT | 号 |
| building_name | TEXT | 建物名 |
| room_no | TEXT | 部屋番号 |

### C. 正規化キー

| カラム名 | 型 | 説明 |
|----------|-----|------|
| addr_std | TEXT | 正規化住所（全角/半角・漢数字→算用数字・スペース統一） |
| addr_key | TEXT | 検索用キー（ひらがな化/記号除去/小文字化） |
| pref_kana | TEXT | 都道府県カナ |
| municipality_kana | TEXT | 市区町村カナ |
| town_kana | TEXT | 町名カナ |
| addr_roman | TEXT | ローマ字住所 |

### D. 緯度経度（既存機能）

| カラム名 | 型 | 説明 |
|----------|-----|------|
| latitude | REAL | 緯度 |
| longitude | REAL | 経度 |
| geocoded_at | TIMESTAMP | ジオコーディング実行時刻 |

### E. 空間グリッド

| カラム名 | 型 | 説明 |
|----------|-----|------|
| geohash_6 | TEXT | 6文字Geohash |
| geohash_8 | TEXT | 8文字Geohash |
| mesh_1km | TEXT | 1kmメッシュコード |
| mesh_500m | TEXT | 500mメッシュコード |
| s2_cell_l12 | TEXT | S2セル（レベル12）※将来実装 |

### F. ヒエラルキー/クラスタ/品質

| カラム名 | 型 | 説明 |
|----------|-----|------|
| admin_path | TEXT | 行政階層（JSON配列） |
| admin_ids | TEXT | 行政ID配列（JSON） |
| postal_code | TEXT | 郵便番号 |
| region_block | TEXT | 地域ブロック |
| address_parse_score | REAL | 住所解析スコア（0.0-1.0） |
| normalize_warnings | TEXT | 正規化警告（JSON配列） |

## 使用方法

### 1. 統合ETL（推奨）

```bash
cd /Users/tsukasa/Arealinks/db_ETL
python integrated_geocoding_etl.py
```

選択肢：
- **処理モード**: 基本ジオコーディングのみ / 拡張統合処理
- **実行規模**: テスト(100件) / 中規模(10,000件) / 全件(640,000件以上)

### 2. 拡張住所処理のみ

```bash
python enhanced_address_etl.py
```

### 3. 基本ジオコーディングのみ

```bash
python create_geocoded_db.py  # 新規DB作成
python address_geocoding_etl.py  # 既存DB更新
```

## データ出力例

### 基本ジオコーディング出力
```
住所: 神奈川県小田原市酒匂４
緯度: 35.267761
経度: 139.193817
```

### 拡張統合処理出力
```
元住所: 神奈川県小田原市酒匂４
正規化住所: 神奈川県小田原市酒匂４
都道府県コード: 14
市区町村名: 小田原市
町名: 酒匂
座標: (35.267761, 139.193817)
GeoHash: xn6bxk
メッシュコード: 523971
解析スコア: 0.6
行政階層: [{"level": "prefecture", "id": "14", "name": "神奈川県"}, {"level": "municipality", "id": "14000", "name": "小田原市"}]
```

## パフォーマンス

- **基本ジオコーディング**: 約150-200件/秒
- **拡張統合処理**: 約100-150件/秒
- **メモリ使用量**: バッチサイズ1,000件で約50MB
- **ディスク容量**: 拡張処理で元DBの約1.5-2倍

## インデックス

以下のインデックスが自動作成されます：

```sql
CREATE INDEX idx_address ON table_name(address);
CREATE INDEX idx_addr_std ON table_name(addr_std);
CREATE INDEX idx_addr_key ON table_name(addr_key);
CREATE INDEX idx_latitude ON table_name(latitude);
CREATE INDEX idx_longitude ON table_name(longitude);
CREATE INDEX idx_pref_code ON table_name(pref_code);
CREATE INDEX idx_geohash_6 ON table_name(geohash_6);
CREATE INDEX idx_mesh_1km ON table_name(mesh_1km);
CREATE INDEX idx_town_name ON table_name(town_name);
CREATE INDEX idx_parse_score ON table_name(address_parse_score);
```

## 拡張機能の活用例

### 1. 地理的検索
```sql
-- 特定のGeoHashエリア内の物件検索
SELECT * FROM BUY_data_integrated
WHERE geohash_6 = 'xn6bxk';

-- 特定メッシュ内の物件検索
SELECT * FROM BUY_data_integrated
WHERE mesh_1km = '523971';
```

### 2. 住所正規化検索
```sql
-- 曖昧な住所検索（カナ検索）
SELECT * FROM BUY_data_integrated
WHERE addr_key LIKE '%おだわら%';
```

### 3. 行政区域検索
```sql
-- 都道府県別集計
SELECT pref_code, COUNT(*)
FROM BUY_data_integrated
GROUP BY pref_code;

-- 市区町村別集計
SELECT municipality_name, COUNT(*)
FROM BUY_data_integrated
GROUP BY municipality_name;
```

### 4. 品質フィルタリング
```sql
-- 高品質住所解析結果のみ
SELECT * FROM BUY_data_integrated
WHERE address_parse_score >= 0.7;
```

## 依存関係

```txt
jageocoder>=2.0.0
pandas
tqdm
jaconv
unicodedata2
```

## 制限事項・注意点

1. **JISコード**: 簡易的な実装のため、正確な市区町村コードは外部データとの照合が必要
2. **郵便番号**: 現在は未実装、将来的に日本郵便データとの照合を予定
3. **S2セル**: 複雑な実装のため保留
4. **カナ・ローマ字**: 基本的なルール変換のみ、完全対応は困難
5. **住所解析**: 正規表現ベースのため、複雑な住所表記では精度が低下する可能性

## 今後の拡張予定

1. 正確なJIS行政コード対応表の統合
2. 日本郵便郵便番号データとの照合
3. S2セル計算の実装
4. より高精度な住所解析アルゴリズム
5. 住所候補の複数提示機能

## トラブルシューティング

### jageocoderエラー
```bash
# 辞書データの再インストール
python -m jageocoder install-dictionary -y [辞書ファイル]
```

### メモリ不足
- バッチサイズを小さくする（例：500件）
- 処理件数を制限する

### 処理速度改善
- SSDの使用
- バッチサイズの最適化
- 並列処理の検討（将来実装）