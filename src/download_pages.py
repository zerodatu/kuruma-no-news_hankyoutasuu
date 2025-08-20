import requests
import os
import sys
import time
import random
import hashlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm
from art import *

# === 設定 ===
BASE_URL = "https://kuruma-news.jp/post/"
MAX_WORKERS = 8  # スレッド数を現実的な値に下げ、サーバー負荷を軽減します
DOWNLOAD_DIR = "download"
DOWNLOAD_SIZE_LIMIT_GB = 25  # フォルダサイズのGB上限
DOWNLOAD_SIZE_LIMIT_BYTES = DOWNLOAD_SIZE_LIMIT_GB * 1024 * 1024 * 1024

WAIT_BETWEEN_REQUESTS = (0.4, 1.2)  # ← ランダムゆらしで優しく
MAX_PAGES_PER_ARTICLE = 40

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


def get_directory_size(directory):
    """指定されたディレクトリの合計サイズをバイト単位で返す"""
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(directory):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                # シンボリックリンクは計算から除外
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
    except FileNotFoundError:
        return 0
    return total_size


def polite_sleep():
    time.sleep(random.uniform(*WAIT_BETWEEN_REQUESTS))


def save_html(text, path):
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def head_exists(url):
    """
    記事の存在を軽く確認する関数
    - 200台なら存在とみなす
    - 301/302/308 などリダイレクトも allow_redirects=True で追従して最終ステータスを判定
    - 405 や HEAD非対応っぽい時は GET(stream=True) にフォールバックして本文は読まない
    """
    try:
        res = SESSION.head(url, timeout=10, allow_redirects=True)
        code = res.status_code
        # 一部サイトは HEAD に 405 を返すことがあるのでフォールバック
        if code in (405, 400):  # 仕様によって調整してね
            res = SESSION.get(url, timeout=10, allow_redirects=True, stream=True)
            code = res.status_code
            # 早めに接続を閉じる
            res.close()
        if 200 <= code < 300:
            return True
        if code in (401, 403, 404, 410):
            return False
        if code >= 500:
            # サーバがつらそうなら今回は見送り
            return False
        # その他は慎重に False
        return False
    except requests.RequestException:
        return False


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
    まず /<id> の存在チェックをしてから本文取得へ進む
    /<id> 本文と /<id>/<n> のページを順に取得し 保存
    """
    base = f"{BASE_URL}{article_id}"

    # 先に存在チェック
    if not head_exists(base):
        print(f"{base} → 存在なしっぽいのでスキップ")
        polite_sleep()
        return False

    last_hash = None
    got_any = False

    for page in range(1, MAX_PAGES_PER_ARTICLE + 1):
        url = base if page == 1 else f"{base}/{page}"
        text, code = fetch(url)

        if text is None:
            if page == 1 and code == 404:
                print(f"{url} → 404 Not Found スキップ")
            elif code in (401, 403):
                print(f"{url} → {code} Forbidden スキップ")
                tprint(f"Forbidden Gundom")
                sys.exit(1)
            elif code >= 500 and code != -1:
                print(f"{url} → {code} Server Error スキップ")
            polite_sleep()
            break

        h = hashlib.md5(text.encode("utf-8")).hexdigest()
        if last_hash is not None and h == last_hash:
            print(f"{url} → 同一コンテンツ検知 終了")
            polite_sleep()
            break
        last_hash = h

        file_path = os.path.join(
            DOWNLOAD_DIR,
            f"{article_id}.html" if page == 1 else f"{article_id}_{page}.html",
        )
        save_html(text, file_path)
        print(f"{url} → 保存完了 {file_path}")

        got_any = True
        polite_sleep()

    return got_any


def main(start_id, end_id):
    # 最初に現在のフォルダサイズをチェック
    initial_size = get_directory_size(DOWNLOAD_DIR)
    if initial_size >= DOWNLOAD_SIZE_LIMIT_BYTES:
        size_gb = initial_size / (1024**3)
        print(f"処理開始前に、すでにフォルダサイズが上限 ({size_gb:.2f}GB / {DOWNLOAD_SIZE_LIMIT_GB}GB) を超えています。")
        return

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [
            executor.submit(download_article_with_paging, i)
            for i in range(start_id, end_id + 1)
        ]

        progress = tqdm(as_completed(futures), total=len(futures), desc="ダウンロード中", unit="記事")
        for fut in progress:
            fut.result()

            current_size = get_directory_size(DOWNLOAD_DIR)
            progress.set_postfix_str(f"Size: {current_size / (1024**3):.2f}GB / {DOWNLOAD_SIZE_LIMIT_GB}GB")
            if current_size >= DOWNLOAD_SIZE_LIMIT_BYTES:
                print(f"\n[停止] フォルダサイズが上限 ({DOWNLOAD_SIZE_LIMIT_GB}GB) に達しました。処理を終了します。")
                break


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("使い方: python download_pages.py <START_ID> <END_ID>")
        sys.exit(1)
    start_id = int(sys.argv[1])
    end_id = int(sys.argv[2])
    t0 = time.time()
    main(start_id, end_id)
    print(f"完了 処理時間: {time.time() - t0:.2f}秒")
