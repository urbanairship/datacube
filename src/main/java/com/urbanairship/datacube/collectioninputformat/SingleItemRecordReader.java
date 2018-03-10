/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.collectioninputformat;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;

/**
 * For internal use by CollectionInputFormat.
 */
public class SingleItemRecordReader extends RecordReader<Writable, NullWritable> {
    private final SingleValueSplit split;
    private boolean nextKeyValueCalled = false;

    public SingleItemRecordReader(SingleValueSplit split) {
        this.split = split;
    }

    @Override
    public void close() throws IOException {
    }

    @Override
    public Writable getCurrentKey() throws IOException, InterruptedException {
        return split.getKey();
    }

    @Override
    public NullWritable getCurrentValue() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void initialize(InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (nextKeyValueCalled) {
            return false;
        } else {
            nextKeyValueCalled = true;
            return true;
        }
    }
}
