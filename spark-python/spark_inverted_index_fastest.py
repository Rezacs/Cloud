import os
import re
import sys
from pyspark import SparkConf, SparkContext

TOKEN_RE = re.compile(r"[a-z0-9]+")

def load_stopwords(sc, path):
    if not path:
        return set()
    try:
        return {
            line.strip().lower()
            for line in sc.textFile(path).collect()
            if line.strip() and not line.strip().startswith("#")
        }
    except Exception:
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                return {
                    line.strip().lower()
                    for line in f
                    if line.strip() and not line.strip().startswith("#")
                }
        return set()

def parse_args():
    if len(sys.argv) < 3 or len(sys.argv) > 5:
        print("Usage: spark_inverted_index_fastest.py <input> <output> [numPartitions] [stopwordsPath]")
        sys.exit(1)

    input_path  = sys.argv[1]
    output_path = sys.argv[2]
    num_partitions  = 4
    stopwords_path  = None

    if len(sys.argv) >= 4:
        try:
            num_partitions = int(sys.argv[3])
        except ValueError:
            stopwords_path = sys.argv[3]

    if len(sys.argv) == 5:
        stopwords_path = sys.argv[4]

    return input_path, output_path, num_partitions, stopwords_path

def main():
    input_path, output_path, num_partitions, stopwords_path = parse_args()

    conf = (
        SparkConf()
        .setAppName("PySpark Inverted Index Fastest No Sort")
        .set("spark.serializer",              "org.apache.spark.serializer.KryoSerializer")
        .set("spark.shuffle.compress",        "true")
        .set("spark.shuffle.spill.compress",  "true")
        .set("spark.rdd.compress",            "true")
        .set("spark.default.parallelism",     str(num_partitions))
        .set("spark.sql.shuffle.partitions",  str(num_partitions))
        .set("spark.python.worker.reuse",     "true")
    )

    sc = SparkContext(conf=conf)
    stopwords_bc = sc.broadcast(load_stopwords(sc, stopwords_path))

    files = sc.wholeTextFiles(input_path, minPartitions=num_partitions)

    def file_to_postings(file_content):
        path, text = file_content
        filename = path.rsplit("/", 1)[-1]
        sw = stopwords_bc.value
        counts = {}
        text = text.lower()
        for match in TOKEN_RE.finditer(text):
            word = match.group(0)
            if word not in sw:
                counts[word] = counts.get(word, 0) + 1
        return [(word, f"{filename}:{count}") for word, count in counts.items()]

    def create_combiner(v):   return v
    def merge_value(acc, v):  return f"{acc} {v}"
    def merge_combiners(a, b): return f"{a} {b}"

    inverted_index = (
        files
        .flatMap(file_to_postings)
        .combineByKey(
            create_combiner,
            merge_value,
            merge_combiners,
            numPartitions=num_partitions
        )
    )

    inverted_index.map(lambda x: f"{x[0]} {x[1]}").saveAsTextFile(output_path)

    stopwords_bc.destroy()
    sc.stop()

if __name__ == "__main__":
    main()
