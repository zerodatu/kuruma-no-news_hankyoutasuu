import os
from bs4 import BeautifulSoup
from collections import defaultdict
from janome.tokenizer import Tokenizer, Token
import csv
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

# 形態素解析器
tokenizer = Tokenizer()

DOWNLOAD_DIR = "download"  # HTML保存フォルダ
MAX_WORKERS = 40  # スレッド数

# 本文候補セレクタ
CONTENT_SELECTORS = [
    ("div", {"class": "article__content"}),  # 新テーマ
    ("div", {"class": "article-body"}),  # 旧テーマ
    ("div", {"class": "entry-content"}),  # WP汎用
    ("div", {"itemprop": "articleBody"}),  # schema.org
]


def pick_article(soup: BeautifulSoup):
    # 1) <article> タグ優先で探す
    art = soup.find("article")
    if art and art.get_text(strip=True):
        # <article> 内の特定クラス
        for sel in CONTENT_SELECTORS:
            el = art.find(*sel)
            if el and el.get_text(strip=True):
                return el
        # 段落が多い場合は <article> 全体を本文とみなす
        if len(art.find_all("p")) >= 3:
            return art

    # 2) グローバル検索
    for name, attrs in CONTENT_SELECTORS:
        el = soup.find(name, attrs=attrs)
        if el and el.get_text(strip=True):
            return el

    # 3) さらに保険 (article, main, itemprop)
    el = soup.select_one('[itemprop="articleBody"], main, article')
    if el and el.get_text(strip=True):
        return el

    # 4) 最後のヒューリスティック
    best = None
    best_p = 0
    for div in soup.find_all("div"):
        p_cnt = len(div.find_all("p"))
        if p_cnt >= 5 and p_cnt > best_p and len(div.get_text(strip=True)) > 300:
            best = div
            best_p = p_cnt
    return best


def extract_words(text: str) -> list[str]:
    """日本語の名詞を抽出"""
    words: list[str] = []
    for token in tokenizer.tokenize(text):
        if isinstance(token, Token) and token.part_of_speech.split(",")[0] == "名詞":
            words.append(token.surface)
    return words


def parse_html_file(file_name: str):
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    try:
        # バイナリ先頭チェックで画像やPDFを弾く
        with open(file_path, "rb") as fb:
            head = fb.read(2048)
            if b"\xff\xd8" in head or b"\x89PNG" in head or b"%PDF" in head:
                print(f"[BIN] Not HTML: {file_name}")
                return None
            fb.seek(0)
            raw = fb.read()

        soup = None
        # lxml優先 → ダメなら標準パーサ
        for parser in ("lxml", "html.parser"):
            try:
                soup = BeautifulSoup(raw, parser)
                break
            except Exception as e:
                print(f"[WARN] Parser fail {parser}: {file_name} → {e}")

        if soup is None:
            print(f"[SKIP] No parser usable: {file_name}")
            return None

        article = pick_article(soup)
        if not article:
            print(f"[SKIP] No article-content: {file_name}")
            return None

        text = article.get_text(separator=" ", strip=True)
        words = extract_words(text)
        print(f"[OK] Parsed: {file_name} → {len(words)} tokens")
        return file_path, set(words)  # 同一記事内は重複排除

    except Exception as e:
        print(f"[ERROR] {file_name}: {e}")
        return None


def main():
    start_time = time.time()
    word_occurrences = defaultdict(list)

    html_files = [f for f in os.listdir(DOWNLOAD_DIR) if f.endswith(".html")]

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(parse_html_file, file) for file in html_files]

        for future in tqdm(as_completed(futures), total=len(futures), desc="解析中"):
            result = future.result()
            if result:
                file_path, words = result
                for word in words:
                    word_occurrences[word].append(file_path)

    # CSV書き出し
    filename = "word_occurrences_local.csv"
    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        writer.writerow(["単語", "出現回数", "ファイルパス"])
        for word, paths in sorted(
            word_occurrences.items(), key=lambda x: len(x[1]), reverse=True
        ):
            writer.writerow([word, len(paths), ", ".join(paths)])

    print(f"CSV出力完了: {filename}")
    print(f"処理時間: {time.time() - start_time:.2f}秒")


if __name__ == "__main__":
    main()
