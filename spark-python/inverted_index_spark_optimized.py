import re
import sys
from pyspark.sql import SparkSession
from pyspark import SparkConf


TOKEN_RE = re.compile(r"[a-z0-9]+")


def tokenize(text):
    return TOKEN_RE.findall(text.lower())


def main():
    if len(sys.argv) < 3 or len(sys.argv) > 4:
        print("Usage: inverted_index_spark_optimized.py <input> <output> [numPartitions]")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]
    num_partitions = int(sys.argv[3]) if len(sys.argv) == 4 else 8

    # --- Key Spark tuning ---
    # serializer: Kryo is significantly faster than default Java serialization
    # shuffle.compress: compress shuffle data (helps on network-bound clusters)
    # rdd.compress: compress cached RDDs
    conf = (SparkConf()
            .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .set("spark.shuffle.compress", "true")
            .set("spark.rdd.compress", "true")
            # Increase shuffle partitions from default 200
            .set("spark.sql.shuffle.partitions", str(num_partitions)))

    spark = SparkSession.builder.appName("Spark Inverted Index (Optimized)").config(conf=conf).getOrCreate()
    sc = spark.sparkContext

    # --- Fix 1: add minPartitions to wholeTextFiles ---
    # Without this, 4472 files can end up in 4472 partitions (or fewer),
    # with uneven load. minPartitions distributes files across workers better.
    # wholeTextFiles is still necessary to get the filename per document.
    files = sc.wholeTextFiles(input_path, minPartitions=num_partitions * 4)

    # --- Fix 2: collapse to a single shuffle pipeline ---
    # OLD pipeline had 3 shuffles: reduceByKey → groupByKey → sortByKey
    # NEW pipeline: one reduceByKey + one sortByKey (the grouping IS the reduceByKey)
    #
    # Step 1: emit (word, "filename:count") directly — no intermediate (word,file) tuples
    # Step 2: reduceByKey merges postings for the same word by string concatenation
    #         using combineByKey so the combiner runs in the map phase (like Hadoop combiner)

    def file_to_postings(file_content):
        """Emit (word, 'filename:count') for every word in a file."""
        path, text = file_content
        filename = path.split("/")[-1]
        counts = {}
        for word in tokenize(text):
            counts[word] = counts.get(word, 0) + 1
        return [(word, f"{filename}:{count}") for word, count in counts.items()]

    # combineByKey is the RDD equivalent of a Hadoop combiner:
    # - createCombiner: start a new list for a word seen for the first time in a partition
    # - mergeValue: add a new posting to an existing list in the same partition
    # - mergeCombiners: merge partition-local lists during the shuffle
    # This avoids groupByKey, which collects ALL values before any merging.
    inverted_index = (
        files
        .flatMap(file_to_postings)                    # (word, "file:count")
        .combineByKey(
            lambda v: [v],                             # createCombiner
            lambda acc, v: acc + [v],                  # mergeValue
            lambda a, b: a + b,                        # mergeCombiners
            numPartitions=num_partitions
        )
        .mapValues(lambda postings: " ".join(sorted(set(postings))))
        .sortByKey(numPartitions=num_partitions)
    )

    inverted_index.map(lambda x: f"{x[0]}\t{x[1]}").saveAsTextFile(output_path)

    spark.stop()


if __name__ == "__main__":
    main()
