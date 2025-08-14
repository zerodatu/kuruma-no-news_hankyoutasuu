import os
import shutil

DOWNLOAD_DIR = "download"
BROKEN_DIR = "broken"

os.makedirs(BROKEN_DIR, exist_ok=True)


def is_valid_html(file_path):
    try:
        # 最初の数バイトをバイナリで確認する
        with open(file_path, "rb") as f:
            head = f.read(2048)
            if b"\xff\xd8" in head or b"\x89PNG" in head:
                return False
        # UTF-8でひらいて、HTMLが存在するかチェック
        with open(file_path, "r", encoding="utf-8") as f:
            text = f.read()
            # 小文字に全部変換して、確認しやすくする。
            lowerd = text.lower()
            if ("<html" in lowerd) or ("<!doctype" in lowerd):
                return True
            else:
                return False

    # そもそもエンコードが狂ってて読めなかったらNGにして弾くのですわよ
    except Exception as e:
        print(f"NG: {file_path} -> {e}")
        return False


def main():
    files = []

    for f in os.listdir(DOWNLOAD_DIR):
        if f.endswith(".html"):
            files.append(f)

    total = len(files)
    bad = 0

    for i, file in enumerate(files, 1):
        path = os.path.join(DOWNLOAD_DIR, file)
        if not is_valid_html(path):
            shutil.move(path, os.path.join(BROKEN_DIR, file))
            bad += 1
            print(f"[{i}/{total}] NG: {path}")
        else:
            print(f"[{i}/{total}] OK: {path}")
    print(f"\n 判定を完了いたしました。 {bad} 件を {BROKEN_DIR} に移動しました。")


if __name__ == "__main__":
    main()
