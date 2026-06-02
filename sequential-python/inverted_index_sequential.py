import os
import re
import sys
from collections import defaultdict, Counter


STOPWORDS_FILE = "/home/hadoop/Cloud/hadoop-java/src/main/resources/stopwords.txt"
TOKEN_RE = re.compile(r"[a-z0-9]+")


def load_stopwords():
    stopwords = set()

    with open(STOPWORDS_FILE, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            word = line.strip().lower()
            if word and not word.startswith("#"):
                stopwords.add(word)

    return stopwords


def tokenize(text):
    return TOKEN_RE.findall(text.lower())


def collect_files(input_path):
    text_files = []

    if os.path.isfile(input_path):
        text_files.append(input_path)
    else:
        for root, _, files in os.walk(input_path):
            for filename in files:
                if filename.endswith(".txt"):
                    text_files.append(os.path.join(root, filename))

    return text_files


def main():
    if len(sys.argv) != 3:
        print("Usage: python3 inverted_index_sequential.py <input_dir> <output_file>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_file = sys.argv[2]

    stopwords = load_stopwords()
    inverted_index = defaultdict(dict)

    files = collect_files(input_path)

    for filepath in files:
        filename = os.path.basename(filepath)

        try:
            with open(filepath, "r", encoding="utf-8", errors="replace") as f:
                text = f.read()
        except Exception as e:
            print(f"Skipping {filepath}: {e}", file=sys.stderr)
            continue

        counts = Counter(
            word for word in tokenize(text)
            if word not in stopwords
        )

        for word, count in counts.items():
            inverted_index[word][filename] = count

    with open(output_file, "w", encoding="utf-8") as out:
        for word in sorted(inverted_index.keys()):
            postings = [
                f"{filename}:{count}"
                for filename, count in sorted(inverted_index[word].items())
            ]
            out.write(f"{word}\t{' '.join(postings)}\n")


if __name__ == "__main__":
    main()