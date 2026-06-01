package it.unipi.cloud;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.fs.FileStatus;

public class WholeFileInputFormat extends CombineFileInputFormat<Text, Text> {

    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        List<FileStatus> files = super.listStatus(job);
        List<FileStatus> nonEmptyFiles = new ArrayList<>();

        for (FileStatus file : files) {
            if (file.getLen() > 0) {
                nonEmptyFiles.add(file);
            }
        }

        return nonEmptyFiles;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    @Override
    public RecordReader<Text, Text> createRecordReader(
            InputSplit split,
            TaskAttemptContext context
    ) throws IOException {
        return new CombineFileRecordReader<>(
                (CombineFileSplit) split,
                context,
                WholeFileRecordReader.class
        );
    }

    public static class WholeFileRecordReader extends RecordReader<Text, Text> {

        private final CombineFileSplit split;
        private final TaskAttemptContext context;
        private final int index;

        private boolean processed = false;
        private final Text key = new Text();
        private final Text value = new Text();

        public WholeFileRecordReader(
                CombineFileSplit split,
                TaskAttemptContext context,
                Integer index
        ) {
            this.split = split;
            this.context = context;
            this.index = index;
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) {
        }

        @Override
        public boolean nextKeyValue() throws IOException {
            if (processed) {
                return false;
            }

            Path path = split.getPath(index);
            FileSystem fs = path.getFileSystem(context.getConfiguration());

            long fileLength = split.getLength(index);
            byte[] contents = new byte[(int) fileLength];

            FSDataInputStream in = null;
            try {
                in = fs.open(path);
                IOUtils.readFully(in, contents, 0, contents.length);
            } finally {
                IOUtils.closeStream(in);
            }

            key.set(path.getName());
            value.set(new String(contents, StandardCharsets.UTF_8));

            processed = true;
            return true;
        }

        @Override
        public Text getCurrentKey() {
            return key;
        }

        @Override
        public Text getCurrentValue() {
            return value;
        }

        @Override
        public float getProgress() {
            return processed ? 1.0f : 0.0f;
        }

        @Override
        public void close() {
        }
    }
}