import re
import sys
import subprocess

STOPWORDS_FILE = "/home/hadoop/Cloud/hadoop-java/src/main/resources/stopwords.txt"


def load_stopwords(stopwords_file):
    stopwords = set()

    with open(stopwords_file, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            word = line.strip().lower()
            if word and not word.startswith("#"):
                stopwords.add(word)

    return stopwords


def normalize_query(query, stopwords):
    terms = re.findall(r"[a-z0-9]+", query.lower())
    return [term for term in terms if term not in stopwords]


def parse_files(postings):
    files = set()

    for item in postings:
        if ":" in item:
            filename = item.rsplit(":", 1)[0]
            files.add(filename)

    return files



def iter_index_lines(index_path):
    """
    Read lines either from a local file or from HDFS.

    Examples:
      local: /home/hadoop/index.txt
      hdfs:  /output/demo/.../part-r-00000
      hdfs:  hdfs:///output/demo/.../part-r-00000
    """

    if index_path.startswith("hdfs://") or index_path.startswith("/output/"):
        hdfs_path = index_path

        # If path starts with hdfs:// keep it.
        # If it starts with /output/... it also works with hdfs dfs -cat.
        process = subprocess.Popen(
            ["hdfs", "dfs", "-cat", hdfs_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace"
        )

        assert process.stdout is not None

        for line in process.stdout:
            yield line

        return_code = process.wait()

        if return_code != 0:
            stderr = process.stderr.read() if process.stderr else ""
            raise RuntimeError(f"Failed to read HDFS path: {hdfs_path}\n{stderr}")

    else:
        with open(index_path, "r", encoding="utf-8", errors="replace") as f:
            for line in f:
                yield line


def load_index(index_file):
    index = {}

    for line in iter_index_lines(index_file):
        parts = line.strip().split()

        if len(parts) < 2:
            continue

        word = parts[0]
        postings = parts[1:]

        index[word] = parse_files(postings)

    return index


def search(index, query, stopwords):
    query_terms = normalize_query(query, stopwords)

    if not query_terms:
        return []

    result = None

    for term in query_terms:
        files = index.get(term, set())

        if result is None:
            result = files
        else:
            result = result.intersection(files)

    return sorted(result) if result else []


def interactive_mode(index, stopwords):
    print("Simple Inverted Index Search")
    print("Type a word or multiple words.")
    print("Stopwords in the query are ignored.")
    print("Type 'exit' or 'quit' to stop.")
    print()

    while True:
        query = input("Search query: ").strip()

        if query.lower() in {"exit", "quit"}:
            break

        results = search(index, query, stopwords)

        for filename in results:
            print(filename)

        if not results:
            print("No matching files found.")

        print()


def main():
    if len(sys.argv) < 2 or len(sys.argv) > 3:
        print("Usage:")
        print("  python3 search_index.py <index_file>")
        print("  python3 search_index.py <index_file> <query>")
        sys.exit(1)

    index_file = sys.argv[1]
    stopwords = load_stopwords(STOPWORDS_FILE)
    index = load_index(index_file)

    if len(sys.argv) == 3:
        query = sys.argv[2]
        results = search(index, query, stopwords)

        for filename in results:
            print(filename)
    else:
        interactive_mode(index, stopwords)


if __name__ == "__main__":
    main()