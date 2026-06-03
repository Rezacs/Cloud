package it.unipi.cloud;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class StopWords {

    public static Set<String> load(Configuration conf) throws IOException {
        Set<String> stopWords = new HashSet<>();

        String stopWordsPath = conf.get("stopwords.path");

        if (stopWordsPath == null || stopWordsPath.trim().isEmpty()) {
            return stopWords;
        }

        Path path = new Path(stopWordsPath);
        FileSystem fs = path.getFileSystem(conf);

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(fs.open(path), StandardCharsets.UTF_8))) {

            String line;

            while ((line = reader.readLine()) != null) {
                String word = line.toLowerCase().replaceAll("[^a-z0-9]", "").trim();

                if (!word.isEmpty()) {
                    stopWords.add(word);
                }
            }
        }

        return stopWords;
    }
}