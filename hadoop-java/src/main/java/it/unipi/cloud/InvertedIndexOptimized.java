package it.unipi.cloud;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Partitioner;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Optimized InvertedIndex — key design change:
 *
 *   OLD: mapper emits  "word@filename" → 1
 *        requires millions of composite key comparisons in sort/shuffle
 *
 *   NEW: mapper emits  "word" → "filename:count"
 *        ~993k unique word keys instead of (words × files) composite keys
 *        combiner can actually merge postings for the same word within one mapper
 *        reducer receives clean (word, [posting, posting, ...]) groups directly
 *
 * Combined with in-mapper aggregation (localCounts per word, not per word@file),
 * this dramatically reduces shuffle data volume.
 */
public class InvertedIndexOptimized {

    /**
     * Mapper: aggregates word counts per file locally (in-mapper combining),
     * then emits word → "filename:count" once per unique word per split.
     *
     * Since each mapper task processes one input split (one or more lines from
     * one file), filename is constant — so the local map is just word → count.
     */
    public static class OptimizedMapper
            extends Mapper<Object, Text, Text, Text> {

        private String filename;
        // word → local count within this split
        private final Map<String, Integer> localCounts = new HashMap<>();
        private final Text outputKey = new Text();
        private final Text outputValue = new Text();
        private static final java.util.regex.Pattern TOKEN_PATTERN =
                java.util.regex.Pattern.compile("[a-z0-9]+");

        @Override
        protected void setup(Context context) {
            FileSplit split = (FileSplit) context.getInputSplit();
            filename = split.getPath().getName();
            localCounts.clear();
        }

        @Override
        public void map(Object key, Text value, Context context) {
            String line = value.toString().toLowerCase();
            java.util.regex.Matcher matcher = TOKEN_PATTERN.matcher(line);
            while (matcher.find()) {
                String word = matcher.group();
                localCounts.merge(word, 1, Integer::sum);
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {
            for (Map.Entry<String, Integer> entry : localCounts.entrySet()) {
                outputKey.set(entry.getKey());
                outputValue.set(filename + ":" + entry.getValue());
                context.write(outputKey, outputValue);
            }
        }
    }

    /**
     * Combiner: merges postings for the same word emitted by the same mapper.
     *
     * With the new key design, the combiner receives:
     *   "cloud" → ["doc1.txt:3", "doc1.txt:2"]   (two splits from same file)
     * and merges them into:
     *   "cloud" → ["doc1.txt:5"]
     *
     * This reduces shuffle traffic significantly when files are split across
     * multiple mapper tasks (which happens for large files > HDFS block size).
     */
    public static class PostingCombiner
            extends Reducer<Text, Text, Text, Text> {

        private final Text outputValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Merge counts for same filename across multiple splits of the same file
            Map<String, Integer> merged = new HashMap<>();
            for (Text val : values) {
                String posting = val.toString();
                int colon = posting.lastIndexOf(':');
                String fname = posting.substring(0, colon);
                int count = Integer.parseInt(posting.substring(colon + 1));
                merged.merge(fname, count, Integer::sum);
            }

            // Emit one "filename:count" per unique file seen in this mapper
            for (Map.Entry<String, Integer> entry : merged.entrySet()) {
                outputValue.set(entry.getKey() + ":" + entry.getValue());
                context.write(key, outputValue);
            }
        }
    }

    /**
     * Reducer: receives word → [all "filename:count" postings across all mappers].
     * Merges any remaining duplicate filenames (from multi-block files), then writes
     * the final inverted index line.
     */
    public static class OptimizedReducer
            extends Reducer<Text, Text, Text, Text> {

        private final Text outputValue = new Text();

        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            // Final merge: file may appear in multiple postings if split across blocks
            Map<String, Integer> postings = new HashMap<>();
            for (Text val : values) {
                String posting = val.toString();
                int colon = posting.lastIndexOf(':');
                String fname = posting.substring(0, colon);
                int count = Integer.parseInt(posting.substring(colon + 1));
                postings.merge(fname, count, Integer::sum);
            }

            // Build output string
            List<String> parts = new ArrayList<>(postings.size());
            for (Map.Entry<String, Integer> e : postings.entrySet()) {
                parts.add(e.getKey() + ":" + e.getValue());
            }
            parts.sort(null);

            outputValue.set(String.join(" ", parts));
            context.write(key, outputValue);
        }
    }

    /**
     * Partitioner: routes all postings for the same word to the same reducer.
     * With word-only keys this is just the default hash partitioner — kept
     * explicit for clarity and to match the original project structure.
     */
    public static class WordPartitioner extends Partitioner<Text, Text> {
        @Override
        public int getPartition(Text key, Text value, int numPartitions) {
            return Math.abs(key.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 2 || args.length > 3) {
            System.err.println("Usage: InvertedIndexOptimized <input> <output> [numReducers]");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hadoop Inverted Index (Optimized)");

        job.setJarByClass(InvertedIndexOptimized.class);

        job.setMapperClass(OptimizedMapper.class);
        job.setCombinerClass(PostingCombiner.class);
        job.setReducerClass(OptimizedReducer.class);
        job.setPartitionerClass(WordPartitioner.class);

        // Map output types differ from reduce output types
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (args.length == 3) {
            job.setNumReduceTasks(Integer.parseInt(args[2]));
        }

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
