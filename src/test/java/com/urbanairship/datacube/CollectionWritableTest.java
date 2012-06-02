/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;
import org.junit.Assert;
import org.junit.Test;

import com.urbanairship.datacube.backfill.CollectionWritable;

public class CollectionWritableTest {
    @Test
    public void basicTest() throws Exception  {
        List<IntWritable> list = new ArrayList<IntWritable>();
        list.add(new IntWritable(1));
        list.add(new IntWritable(2));
        list.add(new IntWritable(3));
        
        CollectionWritable c = new CollectionWritable(IntWritable.class, list);
        
        DataOutputBuffer buf = new DataOutputBuffer();
        c.write(buf);
        
        DataInputBuffer in = new DataInputBuffer();
        in.reset(buf.getData(), buf.getLength());
        CollectionWritable roundTripped = new CollectionWritable();
        roundTripped.readFields(in);
        
        Assert.assertEquals(list, roundTripped.getCollection());
        Iterator<? extends Writable> it = roundTripped.getCollection().iterator();
        Assert.assertEquals(new IntWritable(1), it.next());
        Assert.assertEquals(new IntWritable(2), it.next());
        Assert.assertEquals(new IntWritable(3), it.next());
        Assert.assertTrue(!it.hasNext());
    }
    
    @Test
    public void emptyCollection() throws Exception {
        Collection<Scan> scans = new HashSet<Scan>();
        DataOutputBuffer buf = new DataOutputBuffer();
        CollectionWritable c = new CollectionWritable(Scan.class, scans);
        c.write(buf);
        
        DataInputBuffer in = new DataInputBuffer();
        in.reset(buf.getData(), buf.getLength());
        CollectionWritable roundTripped = new CollectionWritable();
        roundTripped.readFields(in);
        
        Assert.assertEquals(0, roundTripped.getCollection().size());
    }
}
