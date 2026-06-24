import os
import re
import sys
import glob
import subprocess

STOPWORDS_FILE = "/home/hadoop/Cloud/hadoop-java/src/main/resources/stopwords.txt"


def load_stopwords(stopwords_file):
    stopwords = set()

    try:
        with open(stopwords_file, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                word = line.strip().lower()
                if word and not word.startswith("#"):
                    stopwords.add(word)
    except FileNotFoundError:
        # If the stopwords file is missing, continue without stopwords.
        pass

    return stopwords


def normalize_text(text):
    return re.findall(r"[a-z0-9]+", text.lower())


def normalize_query(query, stopwords):
    terms = normalize_text(query)
    return [term for term in terms if term not in stopwords]


def is_hdfs_path(path):
    return (
        path.startswith("hdfs://")
        or path.startswith("/user/")
        or path.startswith("/input/")
        or path.startswith("/output/")
        or path.startswith("/datasets/")
    )


def get_local_files(path):
    """
    Return local files from:
      - a single file
      - a directory
      - a wildcard pattern
    """

    files = []

    if os.path.isfile(path):
        files.append(path)

    elif os.path.isdir(path):
        for root, _, filenames in os.walk(path):
            for filename in filenames:
                files.append(os.path.join(root, filename))

    else:
        matched_files = glob.glob(path)
        for file_path in matched_files:
            if os.path.isfile(file_path):
                files.append(file_path)

    return sorted(files)


def get_hdfs_files(path):
    """
    Return HDFS files from:
      - a single HDFS file
      - a HDFS directory
      - a HDFS wildcard pattern
    """

    process = subprocess.Popen(
        ["hdfs", "dfs", "-ls", path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace"
    )

    stdout, stderr = process.communicate()

    if process.returncode != 0:
        raise RuntimeError(f"Failed to list HDFS path: {path}\n{stderr}")

    files = []

    for line in stdout.splitlines():
        parts = line.split()

        if len(parts) < 8:
            continue

        permissions = parts[0]
        file_path = parts[-1]

        # Skip directories.
        if permissions.startswith("d"):
            continue

        files.append(file_path)

    return sorted(files)


def read_local_file(file_path):
    with open(file_path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def read_hdfs_file(file_path):
    process = subprocess.Popen(
        ["hdfs", "dfs", "-cat", file_path],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding="utf-8",
        errors="replace"
    )

    stdout, stderr = process.communicate()

    if process.returncode != 0:
        raise RuntimeError(f"Failed to read HDFS file: {file_path}\n{stderr}")

    return stdout


def filename_only(path):
    return os.path.basename(path)


def file_contains_all_terms(text, query_terms):
    words = set(normalize_text(text))
    return all(term in words for term in query_terms)


def search_raw_files(input_path, query, stopwords):
    query_terms = normalize_query(query, stopwords)

    if not query_terms:
        return []

    results = []

    if is_hdfs_path(input_path):
        files = get_hdfs_files(input_path)

        for file_path in files:
            text = read_hdfs_file(file_path)

            if file_contains_all_terms(text, query_terms):
                results.append(filename_only(file_path))

    else:
        files = get_local_files(input_path)

        for file_path in files:
            text = read_local_file(file_path)

            if file_contains_all_terms(text, query_terms):
                results.append(filename_only(file_path))

    return sorted(results)


def main():
    if len(sys.argv) != 3:
        print("Usage:")
        print("  python3 raw_search.py <input_path> <query>")
        print()
        print("Examples:")
        print('  python3 raw_search.py /home/hadoop/input "cloud computing"')
        print('  python3 raw_search.py "/home/hadoop/input/*.txt" "cloud computing"')
        print('  python3 raw_search.py /input/news "cloud computing"')
        print('  python3 raw_search.py "/input/news/*" "cloud computing"')
        sys.exit(1)

    input_path = sys.argv[1]
    query = sys.argv[2]

    stopwords = load_stopwords(STOPWORDS_FILE)

    results = search_raw_files(input_path, query, stopwords)

    for filename in results:
        print(filename)


if __name__ == "__main__":
    main()