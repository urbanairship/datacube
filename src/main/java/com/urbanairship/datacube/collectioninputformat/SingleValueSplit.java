package com.urbanairship.datacube.collectioninputformat;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

/**
 * For internal use by CollectionInputFormat.
 */
public class SingleValueSplit extends InputSplit implements Writable {
    private Writable key;
    private Class<? extends Writable> keyClass;
    
    public SingleValueSplit() { } // No-op constructor needed for deserialization
    
    public SingleValueSplit(Class<? extends Writable> keyClass, Writable key) {
        this.keyClass = keyClass;
        this.key = key;
    }
    
    @Override
    public long getLength() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public String[] getLocations() throws IOException, InterruptedException {
        return new String[] {};
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput in) throws IOException {
        String keyClassName = in.readUTF();
        try {
            keyClass = (Class<? extends Writable>)Class.forName(keyClassName);
            key = keyClass.newInstance();
        } catch (Exception e) {
            throw new IOException(e);
        }
        key.readFields(in);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(keyClass.getName());
        key.write(out);
    }
    
    public Writable getKey() {
        return key;
    }
}