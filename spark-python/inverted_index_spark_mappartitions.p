import os
import re
import sys
from pyspark import SparkConf, SparkContext

TOKEN_RE = re.compile(r"[a-z0-9]+")

def load_stopwords(sc, path):
    if not path:
        return set()

    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return {
                line.strip().lower()
                for line in f
                if line.strip() and not line.strip().startswith("#")
            }

    return {
        line.strip().lower()
        for line in sc.textFile(path).collect()
        if line.strip() and not line.strip().startswith("#")
    }

def parse_args():
    if len(sys.argv) < 3 or len(sys.argv) > 5:
        print("Usage: inverted_index_spark_mappartitions.py <input> <output> [numPartitions] [stopwordsPath]")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    num_partitions = 8
    stopwords_path = None

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
            text = text.lower()

            counts = {}
            for match in TOKEN_RE.finditer(text):
                word = match.group(0)
                if word not in stopwords:
                    counts[word] = counts.get(word, 0) + 1

            for word, count in counts.items():
                posting = f"{filename}:{count}"
                if word in grouped:
                    grouped[word].append(posting)
                else:
                    grouped[word] = [posting]

        for word, postings in grouped.items():
            yield word, postings

    def merge_lists(a, b):
        a.extend(b)
        return a

    inverted_index = (
        files
        .mapPartitions(partition_to_postings)
        .reduceByKey(merge_lists, numPartitions=num_partitions)
        .mapValues(lambda postings: " ".join(sorted(postings)))
    )

    inverted_index.map(lambda x: f"{x[0]}\t{x[1]}").saveAsTextFile(output_path)

    stopwords_bc.destroy()
    sc.stop()

if __name__ == "__main__":
    main()