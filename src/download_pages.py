import requests
import os
import sys
import time
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === 設定 ===
BASE_URL = "https://kuruma-news.jp/post/"
MAX_WORKERS = 6            # 控えめにね
DOWNLOAD_DIR = "download"
WAIT_BETWEEN_REQUESTS = (0.6, 1.2)  # ランダム間隔で優しく

# ヘッダをそれっぽく
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.8",
    "Referer": "https://www.google.com/",
    "Connection": "keep-alive",
}

# セッションにリトライ設定
def make_session():
    sess = requests.Session()
    retry = Retry(
        total=5,
        connect=3,
        read=3,
        backoff_factor=1.0,             # 1s, 2s, 4s...の指数バックオフ
        status_forcelist=(403, 429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update(HEADERS)
    return sess

os.makedirs(DOWNLOAD_DIR, exist_ok=True)
SESSION = make_session()

def polite_sleep():
    time.sleep(random.uniform(*WAIT_BETWEEN_REQUESTS))

def download_page(article_id):
    url = f"{BASE_URL}{article_id}"
    file_path = os.path.join(DOWNLOAD_DIR, f"{article_id}.html")
    try:
        res = SESSION.get(url, timeout=15)

        # 明示的にハンドリング
        if res.status_code == 404:
            print(f"{url} → 404 Not Found スキップ")
            polite_sleep()
            return None
        if res.status_code in (401, 403):
            print(f"{url} → {res.status_code} Forbidden たぶんBOT弾き スキップ")
            polite_sleep()
            return None
        if res.status_code >= 500:
            print(f"{url} → {res.status_code} Server Error スキップ")
            polite_sleep()
            return None

        res.raise_for_status()

        with open(file_path, "w", encoding="utf-8") as f:
            f.write(res.text)

        print(f"{url} → 保存完了")
        polite_sleep()
        return file_path

    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        polite_sleep()
        return None

def main(start_id, end_id):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(download_page, i) for i in range(start_id, end_id + 1)]
        for fut in as_completed(futures):
            fut.result()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("使い方: python download_pages.py <START_ID> <END_ID>")
        sys.exit(1)
    start_id = int(sys.argv[1])
    end_id = int(sys.argv[2])
    t0 = time.time()
    main(start_id, end_id)
    print(f"完了 処理時間: {time.time() - t0:.2f}秒")
