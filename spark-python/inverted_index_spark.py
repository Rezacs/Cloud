import re
import sys
from pyspark.sql import SparkSession


def tokenize(text):
    return re.findall(r"[a-z0-9]+", text.lower())


def main():
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: inverted_index_spark.py <input> <output> [numPartitions]")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    num_partitions = int(sys.argv[3]) if len(sys.argv) == 4 else None

    spark = SparkSession.builder.appName("Spark Inverted Index").getOrCreate()
    sc = spark.sparkContext

    files = sc.wholeTextFiles(input_path)

    word_file_pairs = files.flatMap(
        lambda file_content: [
            ((word, file_content[0].split("/")[-1]), 1)
            for word in tokenize(file_content[1])
        ]
    )

    if num_partitions:
        word_file_counts = word_file_pairs.reduceByKey(lambda a, b: a + b, num_partitions)
    else:
        word_file_counts = word_file_pairs.reduceByKey(lambda a, b: a + b)

    word_postings = word_file_counts.map(
        lambda x: (x[0][0], f"{x[0][1]}:{x[1]}")
    )

    if num_partitions:
        inverted_index = (
            word_postings
            .groupByKey(num_partitions)
            .mapValues(lambda postings: " ".join(sorted(postings)))
            .sortByKey(numPartitions=num_partitions)
        )
    else:
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