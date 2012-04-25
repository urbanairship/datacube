package com.urbanairship.datacube.collectioninputformat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.urbanairship.datacube.backfill.CollectionWritable;

/**
 * A Hadoop InputFormat that lets you use an arbitrary Collection as input to a mapreduce job.
 * Each item in the input Collection will be given to a single map() call as its key. 
 */
public class CollectionInputFormat extends InputFormat<Writable,NullWritable> {
    public static final String CONFKEY_COLLECTION = "collectioninputformat.collectionitems";
    public static final String CONFKEY_VALCLASS = "collectioninputformat.valueclass";
    
    /**
     * Stores the given collection as a configuration value. When getSplits() runs later,
     * it will be able to deserialize the collection and use it.
     */
    public static <T extends Writable> void setCollection(Job job, Class<T> valueClass, 
            Collection<T> collection) throws IOException {
        setCollection(job.getConfiguration(), valueClass, collection);
    }
    
    public static <T extends Writable> void setCollection(Configuration conf, Class<T> valueClass, 
            Collection<T> collection) throws IOException {
        conf.set(CONFKEY_COLLECTION, toBase64(valueClass, collection));
        conf.set(CONFKEY_VALCLASS, valueClass.getName());
    }
    
    public static <T extends Writable> String toBase64(Class<T> valueClass, 
            Collection<? extends Writable> collection) throws IOException {
        DataOutputBuffer out = new DataOutputBuffer();
        new CollectionWritable(valueClass, collection).write(out);
        byte[] rawBytes = Arrays.copyOf(out.getData(), out.getLength());
        return new String(Base64.encodeBase64(rawBytes));
    }
    
    @SuppressWarnings("unchecked")
    public static <T extends Writable> Collection<T> fromBase64(Class<T> valueClass,
            String base64) throws IOException {
        byte[] rawBytes = Base64.decodeBase64(base64.getBytes());
        DataInputBuffer in = new DataInputBuffer();
        in.reset(rawBytes, rawBytes.length);
        
        CollectionWritable cw = new CollectionWritable();
        cw.readFields(in);
        return (Collection<T>)cw.getCollection();
    }
    
    @Override
    public RecordReader<Writable, NullWritable> createRecordReader(final InputSplit split, TaskAttemptContext arg1)
            throws IOException, InterruptedException {
        return new SingleItemRecordReader((SingleValueSplit)split);

    }

    @SuppressWarnings("unchecked")
    @Override
    public List<InputSplit> getSplits(JobContext ctx) throws IOException, InterruptedException {
        String base64 = ctx.getConfiguration().get(CONFKEY_COLLECTION);
        String valueClassName = ctx.getConfiguration().get(CONFKEY_VALCLASS);
        Class<? extends Writable> valueClass;
        try {
            valueClass = (Class<? extends Writable>)Class.forName(valueClassName);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        Collection<? extends Writable> collection = fromBase64(valueClass, base64);
        List<InputSplit> splits = new ArrayList<InputSplit>(collection.size());
        for(Writable val: collection) {
            splits.add(new SingleValueSplit(valueClass, val));
        }
        return splits;
    }
}
