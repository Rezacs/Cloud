package it.unipi.cloud;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class InvertedIndexInMapper {

    public static class InMapperCombinerMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private String filename;
        private Map<String, Integer> localCounts;

        @Override
        protected void setup(Context context) {
            FileSplit split = (FileSplit) context.getInputSplit();
            filename = split.getPath().getName();
            localCounts = new HashMap<>();
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            java.util.regex.Matcher matcher =
                    java.util.regex.Pattern.compile("[a-z0-9]+").matcher(line);

            while (matcher.find()) {
                String word = matcher.group();
                String outputKey = word + "@" + filename;
                localCounts.put(outputKey, localCounts.getOrDefault(outputKey, 0) + 1);
            }
        }

        @Override
        protected void cleanup(Context context)
                throws IOException, InterruptedException {

            Text outputKey = new Text();
            IntWritable outputValue = new IntWritable();

            for (Map.Entry<String, Integer> entry : localCounts.entrySet()) {
                outputKey.set(entry.getKey());
                outputValue.set(entry.getValue());
                context.write(outputKey, outputValue);
            }
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

    public static void main(String[] args) throws Exception {

        if (args.length != 2) {
            System.err.println("Usage: InvertedIndexInMapper <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hadoop Inverted Index with In-Mapper Combining");

        job.setJarByClass(InvertedIndexInMapper.class);

        job.setMapperClass(InMapperCombinerMapper.class);
        job.setReducerClass(IndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}