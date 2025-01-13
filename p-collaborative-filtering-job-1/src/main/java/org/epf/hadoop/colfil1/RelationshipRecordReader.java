package org.epf.hadoop.colfil1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.LineRecordReader;

import java.io.IOException;

public class RelationshipRecordReader extends RecordReader<LongWritable, Relationship> {
    private LineRecordReader lineRecordReader = new LineRecordReader();
    private LongWritable currentKey = new LongWritable();
    private Relationship currentValue = new Relationship();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
        lineRecordReader.initialize(split, context);
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        boolean hasNext = lineRecordReader.nextKeyValue();
        if (hasNext) {
            // If the input split `hasNext` (i.e. has more data to read),
            // we will set the current key to the line number
            // and the current value to an ad-hoc Relationship object.

            // The framework will automatically, thanks to the
            // `getCurrentKey` and `getCurrentValue` methods defined below,
            // get the necessary data to inject into the mapper at the right time.

            // Read line number from `lineRecordReader` and update current key
            currentKey.set(lineRecordReader.getCurrentKey().get());

            // Read line data and update current value
            // HINT: What methods can you call on `lineRecordReader`?
            String currentLine = lineRecordReader.getCurrentValue().toString();
            String user1 = currentLine.split("<->")[0];
            String user2 = currentLine.split("<->")[1];
            user2 = user2.split(",")[0];
            currentValue.setId1(user1);
            currentValue.setId2(user2);
        }
        return hasNext;
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return currentKey;
    }

    @Override
    public Relationship getCurrentValue() throws IOException, InterruptedException {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return lineRecordReader.getProgress();
    }

    @Override
    public void close() throws IOException {
        lineRecordReader.close();
    }
}