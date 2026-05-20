package it.unipi.cloud;

import java.io.IOException;
import java.util.*;

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

public class InvertedIndex {

    public static class IndexMapper
            extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private Text outputKey = new Text();
        private String filename;

        @Override
        protected void setup(Context context) {
            FileSplit split = (FileSplit) context.getInputSplit();
            filename = split.getPath().getName();
        }

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String line = value.toString().toLowerCase();
            String[] words = line.split("\\W+");

            for (String word : words) {
                if (!word.isEmpty()) {
                    outputKey.set(word + "@" + filename);
                    context.write(outputKey, one);
                }
            }
        }
    }

    public static class SumCombiner
            extends Reducer<Text, IntWritable, Text, IntWritable> {

        private IntWritable result = new IntWritable();

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }

            result.set(sum);
            context.write(key, result);
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
            System.err.println("Usage: InvertedIndex <input> <output>");
            System.exit(1);
        }

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Hadoop Inverted Index");

        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(IndexMapper.class);
        job.setCombinerClass(SumCombiner.class);
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