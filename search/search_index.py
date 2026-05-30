import re
import sys


def normalize_query(query):
    return re.findall(r"[a-z0-9]+", query.lower())


def parse_files(postings):
    files = set()

    for item in postings:
        if ":" in item:
            filename = item.rsplit(":", 1)[0]
            files.add(filename)

    return files


def load_index(index_file):
    index = {}

    with open(index_file, "r", encoding="utf-8", errors="replace") as f:
        for line in f:
            parts = line.strip().split()

            if len(parts) < 2:
                continue

            word = parts[0]
            postings = parts[1:]

            index[word] = parse_files(postings)

    return index


def search(index, query):
    query_terms = normalize_query(query)

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


def interactive_mode(index):
    print("Simple Inverted Index Search")
    print("Type a word or multiple words.")
    print("Type 'exit' or 'quit' to stop.")
    print()

    while True:
        query = input("Search query: ").strip()

        if query.lower() in {"exit", "quit"}:
            break

        results = search(index, query)

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
    index = load_index(index_file)

    if len(sys.argv) == 3:
        query = sys.argv[2]
        results = search(index, query)

        for filename in results:
            print(filename)
    else:
        interactive_mode(index)


if __name__ == "__main__":
    main()