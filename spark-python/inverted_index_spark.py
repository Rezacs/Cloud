import os
import re
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession


TOKEN_RE = re.compile(r"[a-z0-9]+", re.IGNORECASE)


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
        print(
            "Usage: inverted_index_spark.py <input> <output> "
            "[numPartitions] [stopwordsPath]"
        )
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
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.shuffle.compress", "true")
        .set("spark.shuffle.spill.compress", "true")
        .set("spark.rdd.compress", "true")
        .set("spark.default.parallelism", str(num_partitions))
        .set("spark.sql.shuffle.partitions", str(num_partitions))
        .set("spark.python.worker.reuse", "true")
    )

    spark = (
        SparkSession.builder
        .appName("Spark Inverted Index Optimized No Sort")
        .config(conf=conf)
        .getOrCreate()
    )

    sc = spark.sparkContext

    stopwords = load_stopwords(sc, stopwords_path)
    stopwords_bc = sc.broadcast(stopwords)

    # Do not overdo this on a tiny cluster.
    # Your best range was p8/p16, so input partitions around 2x output partitions is enough.
    files = sc.wholeTextFiles(input_path, minPartitions=num_partitions * 2)

    def file_to_postings(file_content):
        path, text = file_content
        filename = path.rsplit("/", 1)[-1]
        sw = stopwords_bc.value

        counts = {}

        for match in TOKEN_RE.finditer(text):
            word = match.group(0).lower()
            if word not in sw:
                counts[word] = counts.get(word, 0) + 1

        for word, count in counts.items():
            yield word, f"{filename}:{count}"

    def create_combiner(v):
        return [v]

    def merge_value(acc, v):
        acc.append(v)
        return acc

    def merge_combiners(a, b):
        a.extend(b)
        return a

    inverted_index = (
        files
        .flatMap(file_to_postings)
        .combineByKey(
            create_combiner,
            merge_value,
            merge_combiners,
            numPartitions=num_partitions
        )
        .mapValues(lambda postings: " ".join(sorted(postings)))
    )

    # Important: no sortByKey here.
    # Removing sortByKey avoids a second wide shuffle.
    inverted_index.map(lambda x: f"{x[0]}\t{x[1]}").saveAsTextFile(output_path)

    stopwords_bc.destroy()
    spark.stop()


if __name__ == "__main__":
    main()