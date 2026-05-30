import re
import sys
from pyspark.sql import SparkSession


def tokenize(text):
    return re.findall(r"[a-z0-9]+", text.lower())


def main():
    if len(sys.argv) != 3:
        print("Usage: inverted_index_spark.py <input> <output>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder.appName("Spark Inverted Index").getOrCreate()
    sc = spark.sparkContext

    files = sc.wholeTextFiles(input_path)

    word_file_pairs = files.flatMap(
        lambda file_content: [
            ((word, file_content[0].split("/")[-1]), 1)
            for word in tokenize(file_content[1])
        ]
    )

    word_file_counts = word_file_pairs.reduceByKey(lambda a, b: a + b)

    word_postings = word_file_counts.map(
        lambda x: (x[0][0], f"{x[0][1]}:{x[1]}")
    )

    inverted_index = (
        word_postings
        .groupByKey()
        .mapValues(lambda postings: " ".join(sorted(postings)))
        .sortByKey()
    )

    inverted_index.map(lambda x: f"{x[0]}\t{x[1]}").saveAsTextFile(output_path)

    spark.stop()


if __name__ == "__main__":
    main()