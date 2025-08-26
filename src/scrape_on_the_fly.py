import requests
import sys
import time
import random
import hashlib
import csv
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from tqdm import tqdm
from bs4 import BeautifulSoup
from collections import defaultdict
from janome.tokenizer import Tokenizer, Token
from art import tprint

# === 設定 (各ファイルから統合) ===
BASE_URL = "https://kuruma-news.jp/post/"
MAX_WORKERS = 8  # ネットワークアクセスを伴うため、サーバー負荷を考慮したスレッド数
WAIT_BETWEEN_REQUESTS = (0.4, 1.2)  # 丁寧なアクセス間隔
MAX_PAGES_PER_ARTICLE = 40
OUTPUT_CSV_NAME = "word_occurrences_live.csv"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ja,en;q=0.8",
    "Referer": "https://www.google.com/",
    "Connection": "keep-alive",
}

# 本文候補セレクタ (from main.py)
CONTENT_SELECTORS = [
    ("div", {"class": "article__content"}),
    ("div", {"class": "article-body"}),
    ("div", {"class": "entry-content"}),
    ("div", {"itemprop": "articleBody"}),
]

# === グローバルオブジェクト ===
tokenizer = Tokenizer()

def make_session():
    # from download_pages.py
    sess = requests.Session()
    retry = Retry(
        total=5, connect=3, read=3, backoff_factor=1.0,
        status_forcelist=(403, 429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET", "HEAD"]),
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=MAX_WORKERS, pool_maxsize=MAX_WORKERS)
    sess.mount("http://", adapter)
    sess.mount("https://", adapter)
    sess.headers.update(HEADERS)
    return sess

SESSION = make_session()

# === ヘルパー関数群 ===

def polite_sleep():
    # from download_pages.py
    time.sleep(random.uniform(*WAIT_BETWEEN_REQUESTS))

def fetch(url):
    # from download_pages.py (modified to return response object)
    try:
        res = SESSION.get(url, timeout=15, allow_redirects=True)
        code = res.status_code
        if code in (404, 401, 403) or code >= 500:
            return None, code
        res.raise_for_status()
        return res, code
    except requests.RequestException as e:
        print(f"Error fetching {url}: {e}")
        return None, -1

def pick_article(soup: BeautifulSoup):
    # from main.py
    art = soup.find("article")
    if art and art.get_text(strip=True):
        for sel in CONTENT_SELECTORS:
            el = art.find(*sel)
            if el and el.get_text(strip=True):
                return el
        if len(art.find_all("p")) >= 3:
            return art
    for name, attrs in CONTENT_SELECTORS:
        el = soup.find(name, attrs=attrs)
        if el and el.get_text(strip=True):
            return el
    el = soup.select_one('[itemprop="articleBody"], main, article')
    if el and el.get_text(strip=True):
        return el
    best = None
    best_p = 0
    for div in soup.find_all("div"):
        p_cnt = len(div.find_all("p"))
        if p_cnt >= 5 and p_cnt > best_p and len(div.get_text(strip=True)) > 300:
            best = div
            best_p = p_cnt
    return best

def extract_words(text: str) -> list[str]:
    # from main.py
    words: list[str] = []
    for token in tokenizer.tokenize(text):
        # janomeが稀に特殊なトークンを生成するケースに備え、より堅牢なチェックを行います。
        # 1. tokenがTokenオブジェクトであること
        # 2. part_of_speech属性が文字列であること (Noneなどではないことを確認)
        if isinstance(token, Token) and isinstance(token.part_of_speech, str):
            parts = token.part_of_speech.split(",")
            # 3. split結果が空でなく、かつ最初の要素が「名詞」であること
            if parts and parts[0] == "名詞":
                words.append(token.surface)
    return words

def parse_html_content(html_content: str, url_for_log: str) -> set[str] | None:
    # from main.py's parse_html_file (modified for in-memory)
    try:
        soup = None
        for parser in ("lxml", "html.parser"):
            try:
                soup = BeautifulSoup(html_content, parser)
                break
            except Exception as e:
                print(f"[WARN] Parser fail {parser}: {url_for_log} -> {e}")
        if soup is None:
            print(f"[SKIP] No parser usable: {url_for_log}")
            return None

        article = pick_article(soup)
        if not article:
            # print(f"[SKIP] No article-content: {url_for_log}") # ログが多すぎるのでコメントアウト
            return None

        text = article.get_text(separator=" ", strip=True)
        words = extract_words(text)
        return set(words)
    except Exception as e:
        print(f"[ERROR] Parsing {url_for_log}: {e}")
        return None

# === コアロジック ===

def process_article(article_id: int):
    # from download_pages.py's download_article_with_paging (modified)
    base_url = f"{BASE_URL}{article_id}"
    last_hash = None
    article_words = set()
    got_any = False

    for page in range(1, MAX_PAGES_PER_ARTICLE + 1):
        url = base_url if page == 1 else f"{base_url}/{page}"
        # 記事の存在確認(HEAD)とコンテンツ取得(GET)を1回のGETリクエストにまとめる
        res, code = fetch(url)

        if res is None:
            # 最初のページ取得時に404が返ってきた場合は、記事が存在しないと判断
            if page == 1 and code == 404:
                pass # ログは出さずに、下のpolite_sleep()とbreakでこの記事をスキップ
            elif code in (401, 403):
                print(f"{url} -> {code} Forbidden. Exiting.")
                tprint(f"Forbidden Gundom")
                # スレッドプール全体を停止させるためのシグナルを返す
                return "FORBIDDEN"
            elif code >= 500 and code != -1:
                print(f"{url} -> {code} Server Error. Skipping article.")
            # 失敗時はスリープして、この記事の処理を中断する
            polite_sleep()
            break

        # バイナリファイルチェック (from detect_invalid_html.py & main.py)
        head = res.content[:100]
        if b"\xff\xd8" in head or b"\x89PNG" in head or b"%PDF" in head:
            print(f"{url} -> Binary file detected. Skipping page.")
            polite_sleep()
            continue

        html_text = res.text
        # HTMLタグ簡易チェック (from detect_invalid_html.py)
        if "<html" not in html_text.lower() and "<!doctype" not in html_text.lower():
            print(f"{url} -> No HTML tag found. Skipping page.")
            polite_sleep()
            continue

        # 同一コンテンツチェック (from download_pages.py)
        h = hashlib.md5(html_text.encode("utf-8")).hexdigest()
        if last_hash is not None and h == last_hash:
            # print(f"{url} -> Duplicate content detected. Finishing article.")
            polite_sleep()
            break
        last_hash = h

        # パースして単語を抽出
        page_words = parse_html_content(html_text, url)
        if page_words:
            article_words.update(page_words)
            got_any = True

        polite_sleep()

    if got_any:
        print(f"Article {article_id} -> OK, {len(article_words)} unique words found.")
        return article_id, article_words
    else:
        return None

# === メイン実行部 ===

def main(start_id, end_id):
    t0 = time.time()
    word_occurrences = defaultdict(list)

    article_ids = list(range(start_id, end_id + 1))

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(process_article, i) for i in article_ids]

        progress = tqdm(as_completed(futures), total=len(futures), desc="記事を解析中", unit="記事")
        for fut in progress:
            try:
                result = fut.result()
                if result == "FORBIDDEN":
                    print("\n[停止] 403 Forbidden を受信したため、処理を中断します。")
                    executor.shutdown(wait=False, cancel_futures=True)
                    break
                if result:
                    article_id, words = result
                    for word in words:
                        word_occurrences[word].append(str(article_id))
            except Exception as e:
                print(f"A worker thread caused an error: {e}")


    if not word_occurrences:
        print("有効な単語が1つも見つかりませんでした。")
        return

    # CSV書き出し
    print(f"\n解析完了。CSVファイル '{OUTPUT_CSV_NAME}' に書き出します...")
    try:
        with open(OUTPUT_CSV_NAME, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.writer(f)
            writer.writerow(["単語", "出現記事数", "記事IDリスト"])
            # 出現記事数でソート
            sorted_words = sorted(
                word_occurrences.items(), key=lambda item: len(item[1]), reverse=True
            )
            for word, ids in sorted_words:
                writer.writerow([word, len(ids), ", ".join(ids)])
        print(f"CSV出力完了: {OUTPUT_CSV_NAME}")
    except IOError as e:
        print(f"CSVファイルの書き出しに失敗しました: {e}")

    print(f"全処理完了 処理時間: {time.time() - t0:.2f}秒")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("使い方: python scrape_on_the_fly.py <START_ID> <END_ID>")
        sys.exit(1)
    try:
        start_id = int(sys.argv[1])
        end_id = int(sys.argv[2])
        if start_id > end_id:
            print("エラー: START_ID は END_ID より大きい値にできません。")
            sys.exit(1)
        main(start_id, end_id)
    except ValueError:
        print("エラー: START_ID と END_ID は整数である必要があります。")
        sys.exit(1)
