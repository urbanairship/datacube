/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.backfill;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.io.Closeables;

import org.apache.hadoop.hbase.client.ResultScanner;

import org.apache.hadoop.fs.Path;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.collectioninputformat.CollectionInputFormat;

/**
 * Backfilling is a 3 step process:
 *  (1) snapshot the live cube data into a so-called snapshot table
 *  (2) do the application-level event processing into a cube that is stored in some non-visible table
 *  (3) Merge the snapshot, the backfill table, and the live cube, making changes to the live cube.
 *  
 *  This MR job does step 3.
 *  
 *  TODO InputSplit locality for mapreduce
 */
public class HBaseBackfillMerger implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(HBaseBackfillMerger.class);
    
    static final String CONFKEY_COLUMN_FAMILY = "hbasebackfiller.cf";
    static final String CONFKEY_LIVECUBE_TABLE_NAME = "hbasebackfiller.liveCubeTableName";
    static final String CONFKEY_SNAPSHOT_TABLE_NAME = "hbasebackfiller.snapshotTableName";
    static final String CONFKEY_BACKFILLED_TABLE_NAME = "hbasebackfiller.backfilledTableName";
    static final String CONFKEY_DESERIALIZER = "hbasebackfiller.deserializerClassName";
    
    private final Configuration conf;
    private final byte[] cubeNameKeyPrefix; 
    private final byte[] liveCubeTableName;
    private final byte[] snapshotTableName;
    private final byte[] backfilledTableName;
    private final byte[] cf;
    private final Class<? extends Deserializer<?>> opDeserializer;
    
    private static final byte ff = (byte)0xFF;
    private static final byte[] fiftyBytesFF = new byte[] {ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff,
            ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff,
            ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff, ff};
    
    public HBaseBackfillMerger(Configuration conf, byte[] cubeNameKeyPrefix, byte[] liveCubeTableName, 
            byte[] snapshotTableName, byte[] backfilledTableName, byte[] cf, 
            Class<? extends Deserializer<?>> opDeserializer) {
        this.conf = conf;
        this.cubeNameKeyPrefix = cubeNameKeyPrefix;
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
        HTable liveCubeHTable = null;
        ResultScanner liveCubeScanner = null;
        try {
            // If the live table is empty, we just efficiently copy the backfill in
            liveCubeHTable = new HTable(conf, liveCubeTableName);
            liveCubeScanner = liveCubeHTable.getScanner(cf);
            boolean liveCubeIsEmpty = !liveCubeScanner.iterator().hasNext();
            liveCubeScanner.close();
            if (liveCubeIsEmpty) {
                log.info("Live cube is empty, running a straight copy from the backfill table");

                HBaseSnapshotter hbaseSnapshotter = new HBaseSnapshotter(conf, backfilledTableName,
                        cf, liveCubeTableName, new Path("/tmp/backfill_snapshot_hfiles"), true,
                        cubeNameKeyPrefix, Bytes.add(cubeNameKeyPrefix, fiftyBytesFF));
                return hbaseSnapshotter.runWithCheckedExceptions();
            }  else {
          
                Job job = new Job(conf);
                backfilledHTable = new HTable(conf, backfilledTableName);
                
                Pair<byte[][],byte[][]> allRegionsStartAndEndKeys = backfilledHTable.getStartEndKeys();
                byte[][] internalSplitKeys = BackfillUtil.getSplitKeys(allRegionsStartAndEndKeys);
                Collection<Scan> scans = scansThisCubeOnly(cubeNameKeyPrefix, internalSplitKeys);
                
                // Get the scans that will cover this table, and store them in the job configuration to be used
                // as input splits.
                
                if(log.isDebugEnabled()) {
                    log.debug("Scans: " + scans);
                }
                CollectionInputFormat.setCollection(job, Scan.class, scans);
                
                // We cannot allow map tasks to retry, or we could increment the same key multiple times.
                job.getConfiguration().set("mapred.map.max.attempts", "1");
    
                job.setJobName("DataCube HBase backfiller");
                job.setJarByClass(HBaseBackfillMerger.class);
                job.getConfiguration().set(CONFKEY_DESERIALIZER, opDeserializer.getName());
                job.setMapperClass(HBaseBackfillMergeMapper.class);
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
            }
        } finally {
            if (liveCubeScanner != null) {
                liveCubeScanner.close();
            }
            if (liveCubeHTable != null) {
                liveCubeHTable.close();
            }
            if (backfilledHTable != null) {
                backfilledHTable.close();
            }
        }
    }
    
    /**
     * Get a collection of Scans, one per region, that cover the range of the table having the given
     * key prefix. Thes will be used as the map task input splits.
     */
    public static List<Scan> scansThisCubeOnly(byte[] keyPrefix, 
            byte[][] splitKeys) throws IOException {
        Scan copyScan = new Scan();
        copyScan.setCaching(5000);
        copyScan.setCacheBlocks(false);
        
        // Hack: generate a key that probably comes after all this cube's keys but doesn't include any
        // keys not belonging to this cube.
        byte[] keyAfterCube = ArrayUtils.addAll(keyPrefix, fiftyBytesFF);
        
        List<Scan> scans = new ArrayList<Scan>();
        Scan scanUnderConstruction = new Scan(copyScan);
        
        for(byte[] splitKey: splitKeys) {
            scanUnderConstruction.setStopRow(splitKey);

            // Coerce scan to only touch keys belonging to this cube
            Scan truncated = truncateScan(scanUnderConstruction, keyPrefix, keyAfterCube);
            if(truncated != null) {
                scans.add(truncated);
            }

            scanUnderConstruction = new Scan(copyScan);
            scanUnderConstruction.setStartRow(splitKey);
        }

        // There's another region from last split key to the end of the table.
        Scan truncated = truncateScan(scanUnderConstruction, keyPrefix, keyAfterCube);
        if(truncated != null) {
            scans.add(truncated);
        }

        return scans;
    }
    
    /**
     * Given a scan and a key range, return a new Scan whose range is truncated to only include keys in 
     * that range. Returns null if the Scan does not overlap the given range. 
     */
    private static final Scan truncateScan(Scan scan, byte[] rangeStart, byte[] rangeEnd) {
        byte[] scanStart = scan.getStartRow();
        byte[] scanEnd = scan.getStopRow();
        
        if(scanEnd.length > 0 && bytesCompare(scanEnd, rangeStart) <= 0) {
            // The entire scan range is before the entire cube key range
            return null;
        } else if(scanStart.length > 0 && bytesCompare(scanStart, rangeEnd) >= 0) {
            // The entire scan range is after the entire cube key range
            return null;
        } else {
            // Now we now that the scan range at least partially overlaps the cube key range.
            Scan truncated;
            try {
                truncated = new Scan(scan); // make a copy, don't modify input scan
            } catch (IOException e) {
                throw new RuntimeException(); // This is not plausible
            }
            
            if(scanStart.length == 0 || bytesCompare(rangeStart, scanStart) > 0) {
                // The scan includes extra keys at the beginning that are not part of the cube. Move 
                // the scan start point so that it only touches keys belonging to the cube.
                truncated.setStartRow(rangeStart);
            }
            if(scanEnd.length == 0 || bytesCompare(rangeEnd, scanEnd) < 0) {
                // The scan includes extra keys at the end that are not part of the cube. Move the
                // scan end point so it only touches keys belonging to the cube.
                truncated.setStopRow(rangeEnd);
            }
            return truncated;
        }
    }
    
    private static int bytesCompare(byte[] key1, byte[] key2) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(key1, key2);
    }
}
