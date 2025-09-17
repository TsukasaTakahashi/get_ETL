#!/usr/bin/env python3
"""
jageocoderのテストスクリプト
"""

import jageocoder

def test_jageocoder():
    """jageocoderの動作テスト"""
    # jageocoder初期化
    print("jageocoderを初期化中...")
    import os
    db_dir = os.path.expanduser("~/.jageocoder")

    try:
        # デフォルトディレクトリを指定してinit
        jageocoder.init(db_dir=db_dir, create=True)
        print("初期化完了")
    except Exception as e:
        print(f"初期化エラー: {e}")
        print("jageocoderの辞書データが必要です")
        return

    test_addresses = [
        "埼玉県川越市大字寺尾",
        "宮城県大崎市古川新堀字高田",
        "神奈川県小田原市酒匂４"
    ]

    print("\njageocoder動作テスト")
    print("=" * 50)

    for address in test_addresses:
        print(f"\n住所: {address}")
        try:
            result = jageocoder.search(address)
            if result and len(result) > 0:
                best_result = result[0]
                latitude = best_result.get('y')
                longitude = best_result.get('x')
                level = best_result.get('level')
                matched = best_result.get('matched')

                print(f"  緯度: {latitude}")
                print(f"  経度: {longitude}")
                print(f"  レベル: {level}")
                print(f"  マッチ部分: {matched}")
            else:
                print("  ジオコーディング結果なし")

        except Exception as e:
            print(f"  エラー: {e}")

if __name__ == "__main__":
    test_jageocoder()