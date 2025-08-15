import requests
import os
import sys
import time
import random
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# === 設定 ===
BASE_URL = "https://kuruma-news.jp/post/"
MAX_WORKERS = 6  # 控えめにね
DOWNLOAD_DIR = "download"
WAIT_BETWEEN_REQUESTS = (0.6, 1.2)  # ランダム間隔で優しく
MAX_PAGES_PER_ARTICLE = 40  # 念のための上限

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.8",
    "Referer": "https://www.google.com/",
    "Connection": "keep-alive",
}


def make_session():
    sess = requests.Session()
    retry = Retry(
        total=5,
        connect=3,
        read=3,
        backoff_factor=1.0,
        status_forcelist=(403, 429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS
    )
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update(HEADERS)
    return sess


os.makedirs(DOWNLOAD_DIR, exist_ok=True)
SESSION = make_session()


def polite_sleep():
    time.sleep(random.uniform(*WAIT_BETWEEN_REQUESTS))


def save_html(text, path):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def fetch(url):
    try:
        res = SESSION.get(url, timeout=15, allow_redirects=True)
        code = res.status_code

        if code == 404:
            return None, 404
        if code in (401, 403):
            return None, code
        if code >= 500:
            return None, code

        res.raise_for_status()
        return res.text, code
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None, -1


def download_article_with_paging(article_id):
    """
    /<id> 本文と /<id>/<n> のページを順に取得して保存
    404 や同一コンテンツ検知で終了
    """
    base = f"{BASE_URL}{article_id}"
    last_hash = None
    got_any = False

    for page in range(1, MAX_PAGES_PER_ARTICLE + 1):
        url = base if page == 1 else f"{base}/{page}"
        text, code = fetch(url)

        if text is None:
            # 1ページ目で404なら記事自体無し
            if page == 1 and code == 404:
                print(f"{url} → 404 Not Found スキップ")
            elif code in (401, 403):
                print(f"{url} → {code} Forbidden たぶんBOT弾き スキップ")
            elif code >= 500 and code != -1:
                print(f"{url} → {code} Server Error スキップ")
            polite_sleep()
            break

        # 同一ハッシュ検知で終了
        h = hashlib.md5(text.encode("utf-8")).hexdigest()
        if last_hash is not None and h == last_hash:
            print(f"{url} → 同一コンテンツ検知 多重ページ無しっぽいので終了")
            polite_sleep()
            break
        last_hash = h

        # 保存
        file_path = os.path.join(
            DOWNLOAD_DIR,
            f"{article_id}.html" if page == 1 else f"{article_id}_{page}.html",
        )
        save_html(text, file_path)
        print(f"{url} → 保存完了 {file_path}")

        got_any = True
        polite_sleep()

        # 次ページが無い目安を軽く見る
        # ざっくりと rel="next" や class に page-numbers が無いっぽい時は次の番号を試して
        # 404 なら終わり 200 なら続行という方針にしてる
        # ここでは追加リクエストはせず 次ループで /n+1 を普通に試すよ

    return got_any


def main(start_id, end_id):
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(download_article_with_paging, i)
            for i in range(start_id, end_id + 1)
        ]
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
