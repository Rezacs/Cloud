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
    if os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return {l.strip().lower() for l in f if l.strip() and not l.strip().startswith("#")}
    return {l.strip().lower() for l in sc.textFile(path).collect() if l.strip() and not l.strip().startswith("#")}

def main():
    if len(sys.argv) < 3:
        print("Usage: inverted_index_spark_v2.py <input> <output> [numPartitions] [stopwordsPath]")
        sys.exit(1)

    input_path  = sys.argv[1]
    output_path = sys.argv[2]
    num_partitions = int(sys.argv[3]) if len(sys.argv) >= 4 else 4
    stopwords_path = sys.argv[4] if len(sys.argv) == 5 else None

    conf = (SparkConf()
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.shuffle.compress", "true")
        .set("spark.rdd.compress", "true")
        .set("spark.hadoop.mapreduce.input.fileinputformat.input.dir.recursive", "true"))

    spark = (SparkSession.builder
        .appName("InvertedIndex-v2")
        .config(conf=conf)
        .getOrCreate())
    sc = spark.sparkContext

    stopwords    = load_stopwords(sc, stopwords_path)
    stopwords_bc = sc.broadcast(stopwords)

    # textFile instead of wholeTextFiles — line-based, far less overhead
    # We embed filename via a separate wholeTextFiles only for the filename mapping
    files = sc.wholeTextFiles(input_path, minPartitions=num_partitions)

    def file_to_postings(file_content):
        path, text = file_content
        filename = os.path.basename(path)
        sw = stopwords_bc.value
        counts = {}
        for word in tokenize(text):
            if word not in sw:
                counts[word] = counts.get(word, 0) + 1
        return [(word, (filename, count)) for word, count in counts.items()]

    inverted_index = (
        files
        .flatMap(file_to_postings)
        .groupByKey(numPartitions=num_partitions)   # simpler, correct
        .mapValues(lambda postings: " ".join(sorted(f"{fn}:{c}" for fn, c in postings)))
        .sortByKey(numPartitions=num_partitions)
    )

    inverted_index.map(lambda x: f"{x[0]}\t{x[1]}").saveAsTextFile(output_path)

    stopwords_bc.destroy()
    spark.stop()

if __name__ == "__main__":
    main()