import re
import sys


def normalize_query(query):
    return [w for w in re.split(r"\W+", query.lower()) if w]


def parse_files(postings):
    files = set()
    for item in postings:
        if ":" in item:
            filename = item.rsplit(":", 1)[0]
            files.add(filename)
    return files


def main():
    if len(sys.argv) != 3:
        print("Usage: python3 search_index.py <index_file> <query>")
        sys.exit(1)

    index_file = sys.argv[1]
    query = sys.argv[2]

    query_terms = normalize_query(query)

    if not query_terms:
        return

    term_to_files = {}

    with open(index_file, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            parts = line.strip().split()
            if len(parts) < 2:
                continue

            word = parts[0]

            if word in query_terms:
                term_to_files[word] = parse_files(parts[1:])

    result = None

    for term in query_terms:
        files = term_to_files.get(term, set())

        if result is None:
            result = files
        else:
            result = result.intersection(files)

    for filename in sorted(result):
        print(filename)


if __name__ == "__main__":
    main()