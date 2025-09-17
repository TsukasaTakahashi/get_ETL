# 住所ジオコーディング ETL

jageocoderを使用して住所データから緯度経度を取得し、データベースに追加するETLプログラム

## セットアップ

```bash
pip install -r requirements.txt
```

## 使用方法

```bash
python address_geocoding_etl.py
```

## 処理内容

1. props.dbから住所データを読み込み
2. jageocoderを使用して住所から緯度経度を取得
3. 新しいカラム（latitude, longitude）をデータベースに追加