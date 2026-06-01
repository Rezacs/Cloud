package it.unipi.cloud;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

// import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
// import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;

public class InvertedIndexInMapper {

    public static class InMapperCombinerMapper
            extends Mapper<Text, Text, Text, IntWritable> {

        private Map<String, Integer> localCounts;
        private Set<String> stopWords;

        @Override
        protected void setup(Context context) throws IOException {
            localCounts = new HashMap<>();
            stopWords = StopWords.load(context.getConfiguration());
        }

        @Override
        public void map(Text key, Text value, Context context)
                throws IOException, InterruptedException {

            localCounts.clear();

            String filename = key.toString();
            String line = value.toString().toLowerCase();

            java.util.regex.Matcher matcher =
                    java.util.regex.Pattern.compile("[a-z0-9]+").matcher(line);

            while (matcher.find()) {
                String word = matcher.group();

                if (!stopWords.contains(word)) {
                    String outputKey = word + "@" + filename;
                    localCounts.put(outputKey, localCounts.getOrDefault(outputKey, 0) + 1);
                }
            }

            flush(context);
        }

        private void flush(Context context)
                throws IOException, InterruptedException {

            Text outputKey = new Text();
            IntWritable outputValue = new IntWritable();

            for (Map.Entry<String, Integer> entry : localCounts.entrySet()) {
                outputKey.set(entry.getKey());
                outputValue.set(entry.getValue());
                context.write(outputKey, outputValue);
            }

            localCounts.clear();
        }
    }

    public static class IndexReducer
            extends Reducer<Text, IntWritable, Text, Text> {

        private String currentWord = null;
        private List<String> postings = new ArrayList<>();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            String[] parts = key.toString().split("@", 2);
            String word = parts[0];
            String filename = parts[1];

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            if (currentWord != null && !word.equals(currentWord)) {
                context.write(new Text(currentWord), new Text(String.join(" ", postings)));
                postings.clear();
            }

            currentWord = word;
            postings.add(filename + ":" + sum);
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            if (currentWord != null && !postings.isEmpty()) {
                context.write(new Text(currentWord), new Text(String.join(" ", postings)));
            }
        }
    }

    public static class WordPartitioner extends Partitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numPartitions) {
            String keyString = key.toString();
            String word = keyString.split("@", 2)[0];
            return Math.abs(word.hashCode()) % numPartitions;
        }
    }

    public static void main(String[] args) throws Exception {

        // if (args.length != 2) {
        //     System.err.println("Usage: InvertedIndexInMapper <input> <output>");
        //     System.exit(1);
        // }

        // for reducers
        if (args.length < 2 || args.length > 4) {
            System.err.println("Usage: InvertedIndexInMapper <input> <output> [numReducers] [stopwordsPath]");
            System.exit(1);
        }

        Configuration conf = new Configuration();

        if (args.length >= 4) {
            conf.set("stopwords.path", args[3]);
        }

        Job job = Job.getInstance(conf, "Hadoop Inverted Index with In-Mapper Combining");

        job.setJarByClass(InvertedIndexInMapper.class);

        job.setMapperClass(InMapperCombinerMapper.class);
        job.setReducerClass(IndexReducer.class);
        job.setPartitionerClass(WordPartitioner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        if (args.length >= 3) {
            job.setNumReduceTasks(Integer.parseInt(args[2]));
        }

        job.setInputFormatClass(WholeFileInputFormat.class);
        WholeFileInputFormat.addInputPath(job, new Path(args[0]));
        WholeFileInputFormat.setMinInputSplitSizeRack(job, 1);
        WholeFileInputFormat.setMinInputSplitSizeNode(job, 1);
        WholeFileInputFormat.setMaxInputSplitSize(job, 128 * 1024 * 1024);
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}