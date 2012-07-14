/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.backfill;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.urbanairship.datacube.Deserializer;

public class HBaseBackfill implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(HBaseBackfill.class);
    
    private final Configuration conf;
    private final HBaseBackfillCallback backfillCallback;
    private final byte[] liveDataTableName;
    private final byte[] snapshotTableName;
    private final byte[] backfillTableName;
    private final byte[] cf;
    Class<? extends Deserializer<?>> opDeserializerCls;

    public HBaseBackfill(Configuration conf, HBaseBackfillCallback backfillCallback, byte[] liveDataTableName,
            byte[] snapshotTableName, byte[] backfillTableName, byte[] cf, 
            Class<? extends Deserializer<?>> opDeserializerCls) {
        
        this.conf = conf;
        this.backfillCallback = backfillCallback;
        this.liveDataTableName = liveDataTableName;
        this.snapshotTableName = snapshotTableName;
        this.backfillTableName = backfillTableName;
        this.cf = cf;
        this.opDeserializerCls = opDeserializerCls;
    }

    @Override
    public void run() {
        try {
            runWithCheckedExceptions();
        } catch (IOException e) {
            log.error("Re-throwing as RuntimeException", e);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * @return whether successful
     */
    public boolean runWithCheckedExceptions() throws IOException {
        HTable liveCubeTable = null;
        try {
                
            HBaseSnapshotter hbaseSnapshotter = new HBaseSnapshotter(conf, liveDataTableName, cf, snapshotTableName, 
                    new Path("/tmp/snapshot_hfiles"), false, null, null);
            try {
                if(!hbaseSnapshotter.runWithCheckedExceptions()) {
                    log.error("Snapshotter failed. Returning false.");
                    return false;
                }
            } catch (InterruptedException e) {
                log.error("Snapshotter interrupted. Returning false.");
                return false;
            }
            long snapshotFinishMs = System.currentTimeMillis();
            
            /*
             * Create a table to backfill into. Use the same region boundaries as the live data table
             */
            HBaseAdmin hbaseAdmin = new HBaseAdmin(conf);
            if(hbaseAdmin.tableExists(backfillTableName)) {
                log.error("Backfill table " + new String(backfillTableName) + " shouldn't already exist");
                return false;
            }
            
            liveCubeTable = new HTable(conf, liveDataTableName);
            byte[][] startKeys = liveCubeTable.getStartKeys();
            
            // The region split keys are all the region start keys, except the first.
            List<byte[]> splitKeys = new ArrayList<byte[]>();
            for(int i=1; i<startKeys.length; i++) {
                splitKeys.add(startKeys[i]);
            }
            
            HColumnDescriptor cfDesc = new HColumnDescriptor(cf);
            cfDesc.setBloomFilterType(BloomType.NONE);
            cfDesc.setCompressionType(Algorithm.NONE); // TODO switch to snappy in 0.92
            cfDesc.setMaxVersions(1);
            HTableDescriptor tableDesc = new HTableDescriptor(backfillTableName);
            tableDesc.addFamily(cfDesc);
            hbaseAdmin.createTable(tableDesc, splitKeys.toArray(new byte[][]{}));
            
            backfillCallback.backfillInto(conf, backfillTableName, cf, snapshotFinishMs);

            HBaseBackfillMerger backfillMerger = new HBaseBackfillMerger(conf, 
                    ArrayUtils.EMPTY_BYTE_ARRAY, liveDataTableName, snapshotTableName, 
                    backfillTableName, cf, opDeserializerCls);
            try {
                if(!backfillMerger.runWithCheckedExceptions()) {
                    log.error("Backfill merger failed. Returning false.");
                }
            } catch (InterruptedException e) {
                log.error("Backfill merger was interrupted, returning false");
                return false;
            }
            
            log.debug("Backfill complete");
            return true;
        } finally {
            if(liveCubeTable != null) {
                liveCubeTable.close();
            }
        }
        
    }
}
