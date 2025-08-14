import requests
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

# === 設定 ===
BASE_URL = "https://kuruma-news.jp/post/"
MAX_WORKERS = 20  # 並列スレッド数
DOWNLOAD_DIR = "download"  # ダウンロード先フォルダ
WAIT_BETWEEN_REQUESTS = 0.2  # サーバー負荷軽減

# フォルダ作成
os.makedirs(DOWNLOAD_DIR, exist_ok=True)


def download_page(article_id):
    """記事をダウンロードしてHTML保存"""
    url = f"{BASE_URL}{article_id}"
    file_path = os.path.join(DOWNLOAD_DIR, f"{article_id}.html")

    try:
        res = requests.get(url, timeout=10)

        # 404や500はスキップ
        if res.status_code == 404:
            print(f"{url} → 404 Not Found (スキップ)")
            return None
        elif res.status_code == 500:
            print(f"{url} → 500 Server Error (スキップ)")
            return None

        res.raise_for_status()

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(res.text)

        print(f"{url} → 保存完了")
        time.sleep(WAIT_BETWEEN_REQUESTS)
        return file_path

    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


def main(start_id, end_id):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(download_page, i) for i in range(start_id, end_id + 1)
        ]

        for future in as_completed(futures):
            future.result()  # 実行結果を確認して例外発生時に捕捉


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("使い方: python download_pages.py <START_ID> <END_ID>")
        sys.exit(1)

    start_id = int(sys.argv[1])
    end_id = int(sys.argv[2])

    start_time = time.time()
    main(start_id, end_id)
    print(f"完了！処理時間: {time.time() - start_time:.2f}秒")
