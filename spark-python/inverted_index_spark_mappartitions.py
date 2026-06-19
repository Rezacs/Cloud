import os, re, sys
from pyspark import SparkConf, SparkContext

TOKEN_RE = re.compile(r"[a-z0-9]+")

def load_stopwords(sc, path):
    if not path:
        return set()
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return {l.strip().lower() for l in f if l.strip() and not l.strip().startswith("#")}
    return {l.strip().lower() for l in sc.textFile(path).collect() if l.strip() and not l.strip().startswith("#")}

def main():
    input_path = sys.argv[1]
    output_path = sys.argv[2]
    num_partitions = int(sys.argv[3]) if len(sys.argv) >= 4 else 8
    stopwords_path = sys.argv[4] if len(sys.argv) >= 5 else None

    conf = (
        SparkConf()
        .setAppName("PySpark Inverted Index MapPartitions")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.shuffle.compress", "true")
        .set("spark.shuffle.spill.compress", "true")
        .set("spark.rdd.compress", "true")
        .set("spark.default.parallelism", str(num_partitions))
        .set("spark.sql.shuffle.partitions", str(num_partitions))
        .set("spark.python.worker.reuse", "true")
    )

    sc = SparkContext(conf=conf)
    stopwords_bc = sc.broadcast(load_stopwords(sc, stopwords_path))

    files = sc.wholeTextFiles(input_path, minPartitions=num_partitions * 2)

    def partition_to_postings(iterator):
        stopwords = stopwords_bc.value
        grouped = {}

        for path, text in iterator:
            filename = path.rsplit("/", 1)[-1]
            counts = {}
            for m in TOKEN_RE.finditer(text.lower()):
                word = m.group(0)
                if word not in stopwords:
                    counts[word] = counts.get(word, 0) + 1

            for word, count in counts.items():
                grouped.setdefault(word, []).append(f"{filename}:{count}")

        for word, postings in grouped.items():
            yield word, postings

    def merge_lists(a, b):
        a.extend(b)
        return a

    result = (
        files
        .mapPartitions(partition_to_postings)
        .reduceByKey(merge_lists, numPartitions=num_partitions)
        .mapValues(lambda postings: " ".join(sorted(postings)))
        .map(lambda x: f"{x[0]}\t{x[1]}")
    )

    result.saveAsTextFile(output_path)
    stopwords_bc.destroy()
    sc.stop()

if __name__ == "__main__":
    main()
