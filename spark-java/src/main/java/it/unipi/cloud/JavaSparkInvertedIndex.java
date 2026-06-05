package it.unipi.cloud;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.Serializable;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import scala.Tuple2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.*;
import org.apache.spark.broadcast.Broadcast;

public class JavaSparkInvertedIndex implements Serializable {

    private static final Pattern TOKEN_RE = Pattern.compile("[a-z0-9]+", Pattern.CASE_INSENSITIVE);

    private static Set<String> loadStopwords(String path) throws Exception {
        Set<String> stopwords = new HashSet<>();
        if (path == null || path.isEmpty()) return stopwords;

        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line;
            while ((line = br.readLine()) != null) {
                String w = line.trim().toLowerCase();
                if (!w.isEmpty() && !w.startsWith("#")) {
                    stopwords.add(w);
                }
            }
        }
        return stopwords;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2 || args.length > 4) {
            System.err.println("Usage: JavaSparkInvertedIndex <input> <output> [numPartitions] [stopwordsPath]");
            System.exit(1);
        }

        String input = args[0];
        String output = args[1];
        int partitions = args.length >= 3 ? Integer.parseInt(args[2]) : 8;
        String stopwordsPath = args.length == 4 ? args[3] : null;

        SparkConf conf = new SparkConf()
                .setAppName("Java Spark Inverted Index No Sort")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.shuffle.compress", "true")
                .set("spark.shuffle.spill.compress", "true")
                .set("spark.rdd.compress", "true")
                .set("spark.default.parallelism", String.valueOf(partitions))
                .set("spark.sql.shuffle.partitions", String.valueOf(partitions));

        JavaSparkContext sc = new JavaSparkContext(conf);

        Broadcast<Set<String>> stopwordsBC = sc.broadcast(loadStopwords(stopwordsPath));

        JavaPairRDD<String, String> files = sc.wholeTextFiles(input, partitions * 2);

        JavaPairRDD<String, String> postings = files.flatMapToPair(file -> {
            String path = file._1();
            String text = file._2();
            String filename = path.substring(path.lastIndexOf("/") + 1);

            Set<String> stopwords = stopwordsBC.value();
            Map<String, Integer> counts = new HashMap<>();

            Matcher matcher = TOKEN_RE.matcher(text);
            while (matcher.find()) {
                String word = matcher.group().toLowerCase();
                if (!stopwords.contains(word)) {
                    counts.put(word, counts.getOrDefault(word, 0) + 1);
                }
            }

            List<Tuple2<String, String>> out = new ArrayList<>();
            for (Map.Entry<String, Integer> e : counts.entrySet()) {
                out.add(new Tuple2<>(e.getKey(), filename + ":" + e.getValue()));
            }
            return out.iterator();
        });

        JavaPairRDD<String, List<String>> index = postings.combineByKey(
                v -> {
                    List<String> list = new ArrayList<>();
                    list.add(v);
                    return list;
                },
                (list, v) -> {
                    list.add(v);
                    return list;
                },
                (a, b) -> {
                    a.addAll(b);
                    return a;
                },
                partitions
        );

        JavaRDD<String> outputLines = index.map(t -> {
            List<String> list = t._2();
            Collections.sort(list);
            return t._1() + "\t" + String.join(" ", list);
        });

        outputLines.saveAsTextFile(output);

        stopwordsBC.destroy();
        sc.close();
    }
}