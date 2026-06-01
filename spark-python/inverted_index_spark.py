import os
import re
import sys
from pyspark import SparkConf
from pyspark.sql import SparkSession


TOKEN_RE = re.compile(r"[a-z0-9]+")


def tokenize(text):
    return TOKEN_RE.findall(text.lower())


def load_stopwords(sc, path):
    if not path:
        return set()

    # Local file path
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return {
                line.strip().lower()
                for line in f
                if line.strip() and not line.strip().startswith("#")
            }

    # HDFS path
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
        .set("spark.rdd.compress", "true")
        .set("spark.sql.shuffle.partitions", str(num_partitions))
    )

    spark = (
        SparkSession.builder
        .appName("Spark Inverted Index Optimized")
        .config(conf=conf)
        .getOrCreate()
    )

    sc = spark.sparkContext

    stopwords = load_stopwords(sc, stopwords_path)
    stopwords_bc = sc.broadcast(stopwords)

    files = sc.wholeTextFiles(input_path, minPartitions=num_partitions * 4)

    def file_to_postings(file_content):
        path, text = file_content
        filename = path.split("/")[-1]
        counts = {}
        sw = stopwords_bc.value

        for word in tokenize(text):
            if word not in sw:
                counts[word] = counts.get(word, 0) + 1

        return [(word, f"{filename}:{count}") for word, count in counts.items()]

    inverted_index = (
        files
        .flatMap(file_to_postings)
        .combineByKey(
            lambda v: [v],
            lambda acc, v: acc.append(v) or acc,
            lambda a, b: a + b,
            numPartitions=num_partitions
        )
        .mapValues(lambda postings: " ".join(sorted(postings)))
        .sortByKey(numPartitions=num_partitions)
    )

    inverted_index.map(lambda x: f"{x[0]}\t{x[1]}").saveAsTextFile(output_path)

    stopwords_bc.destroy()
    spark.stop()


if __name__ == "__main__":
    main()