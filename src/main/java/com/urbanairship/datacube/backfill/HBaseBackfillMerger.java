package com.urbanairship.datacube.backfill;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.collectioninputformat.CollectionInputFormat;

/**
 * Backfilling is a 3 step process:
 *  (1) snapshot the live cube data into a so-called snapshot table
 *  (2) do the application-level event processing into a cube that is stored in some non-visible table
 *  (3) Merge the snapshot, the backfill table, and the live cube, making changes to the live cube.
 *  
 *  This MR job does step 3.
 */
public class HBaseBackfillMerger implements Runnable {
    private static final Logger log = LogManager.getLogger(HBaseBackfillMerger.class);
    
    static final String CONFKEY_COLUMN_FAMILY = "hbasebackfiller.cf";
    static final String CONFKEY_LIVECUBE_TABLE_NAME = "hbasebackfiller.liveCubeTableName";
    static final String CONFKEY_SNAPSHOT_TABLE_NAME = "hbasebackfiller.snapshotTableName";
    static final String CONFKEY_BACKFILLED_TABLE_NAME = "hbasebackfiller.backfilledTableName";
    static final String CONFKEY_DESERIALIZER = "hbasebackfiller.deserializerClassName";
    
    private final Configuration conf;
    private final byte[] liveCubeTableName;
    private final byte[] snapshotTableName;
    private final byte[] backfilledTableName;
    private final byte[] cf;
    private final Class<? extends Deserializer<?>> opDeserializer;
    
    public HBaseBackfillMerger(Configuration conf, byte[] liveCubeTableName, byte[] snapshotTableName,
            byte[] backfilledTableName, byte[] cf, Class<? extends Deserializer<?>> opDeserializer) {
        this.conf = conf;
        this.liveCubeTableName = liveCubeTableName;
        this.snapshotTableName = snapshotTableName;
        this.backfilledTableName = backfilledTableName;
        this.cf = cf;
        this.opDeserializer = opDeserializer;
    }

    /**
     * A wrapper around {@link #runWithCheckedExceptions()} that rethrows IOExceptions as
     * RuntimeExceptions.
     */
    @Override
    public void run() {
        try {
            this.runWithCheckedExceptions();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public boolean runWithCheckedExceptions() throws IOException, InterruptedException {
        HTable backfilledHTable = null;
        try {
            Job job = new Job(conf);
            backfilledHTable = new HTable(conf, backfilledTableName);
            
            // Get the scans that will cover this table, and store them in the job configuration to be used
            // as input splits.
            Collection<Scan> scans = getScans(backfilledHTable);
            if(log.isDebugEnabled()) {
                log.debug("Scans: " + scans);
            }
            CollectionInputFormat.setCollection(job, Scan.class, scans);
            
            // We cannot allow map tasks to retry, or we could increment the same key multiple times.
            job.getConfiguration().set("mapred.map.max.attempts", "1");

            job.setJobName("DataCube HBase backfiller");
            job.setJarByClass(HBaseBackfillMerger.class);
            job.getConfiguration().set(CONFKEY_DESERIALIZER, opDeserializer.getName());
            job.setMapperClass(HBaseBackfillMapper.class);
            job.setInputFormatClass(CollectionInputFormat.class);
            job.setNumReduceTasks(0); // No reducers, mappers do all the work
            job.setOutputFormatClass(NullOutputFormat.class); 
            job.getConfiguration().set(CONFKEY_LIVECUBE_TABLE_NAME, new String(liveCubeTableName));
            job.getConfiguration().set(CONFKEY_SNAPSHOT_TABLE_NAME, new String(snapshotTableName));
            job.getConfiguration().set(CONFKEY_BACKFILLED_TABLE_NAME, new String(backfilledTableName));
            job.getConfiguration().set(CONFKEY_COLUMN_FAMILY, new String(cf));
            job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");
            job.getConfiguration().set("mapred.reduce.tasks.speculative.execution", "false");
            
            try {
                job.waitForCompletion(true);
                return job.isSuccessful();
            } catch (ClassNotFoundException e) {
                log.error("", e);
                throw new RuntimeException(e);
            }
        } finally {
            if(backfilledHTable != null) {
                backfilledHTable.close();
            }
        }
    }
    
    /**
     * Get a collection of Scans, one per region, that cover the entire table.
     */
    public static Collection<Scan> getScans(HTable hTable) throws IOException {
        Pair<byte[][],byte[][]> startAndEndKeys = hTable.getStartEndKeys();
        byte[][] startKeys = startAndEndKeys.getFirst();
        byte[][] endKeys = startAndEndKeys.getSecond();
        
        if(startKeys.length != endKeys.length) {
            throw new RuntimeException("Expected same number of start keys and end keys");
        }
        
        Collection<Scan> scans = new ArrayList<Scan>(startKeys.length); 
        for(int i=0; i<startKeys.length; i++) {
            Scan scan = new Scan();
            scan.setStartRow(startKeys[i]);
            scan.setStopRow(endKeys[i]);
            scan.setCacheBlocks(false);
            scan.setCaching(5000);
            scans.add(scan);
        }
        return scans;
    }
}
