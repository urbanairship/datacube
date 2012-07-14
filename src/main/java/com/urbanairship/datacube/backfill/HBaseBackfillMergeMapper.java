/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.backfill;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.urbanairship.datacube.AutoResumeResultScanner;
import com.urbanairship.datacube.DebugHack;
import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.MergeIterator;
import com.urbanairship.datacube.Op;
import com.urbanairship.datacube.ResultComparator;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;

public class HBaseBackfillMergeMapper extends Mapper<Scan,NullWritable,NullWritable,NullWritable> {
    private static final Logger log = LoggerFactory.getLogger(HBaseBackfillMergeMapper.class);
    
    public static enum Ctrs {ACTION_DELETED, ACTION_OVERWRITTEN, ACTION_UNCHANGED,
        ROWS_CHANGED_SINCE_SNAPSHOT, ROWS_NEW_SINCE_SNAPSHOT}; 
    
    @Override
    protected void map(Scan scan, NullWritable ignored, Context context) 
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        byte[] liveCubeTableName = conf.get(HBaseBackfillMerger.CONFKEY_LIVECUBE_TABLE_NAME).getBytes();
        byte[] snapshotTableName = conf.get(HBaseBackfillMerger.CONFKEY_SNAPSHOT_TABLE_NAME).getBytes();
        byte[] backfilledTableName = conf.get(HBaseBackfillMerger.CONFKEY_BACKFILLED_TABLE_NAME).getBytes();
        byte[] cf = conf.get(HBaseBackfillMerger.CONFKEY_COLUMN_FAMILY).getBytes();

        Deserializer<?> deserializer = getDeserializer(conf);
        
        HTable liveCubeHTable = null;
        HTable snapshotHTable = null;
        HTable backfilledHTable = null;
        
        ResultScanner liveCubeScanner = null;
        ResultScanner snapshotScanner = null;
        ResultScanner backfilledScanner = null;

        try {
            liveCubeHTable = new HTable(conf, liveCubeTableName);
            snapshotHTable = new HTable(conf, snapshotTableName);
            backfilledHTable = new HTable(conf, backfilledTableName);

            liveCubeScanner = new AutoResumeResultScanner(liveCubeHTable, scan);
            snapshotScanner = new AutoResumeResultScanner(snapshotHTable, scan);
            backfilledScanner = new AutoResumeResultScanner(backfilledHTable, scan);
            
            Iterator<Result> liveCubeIterator = liveCubeScanner.iterator();
            Iterator<Result> snapshotIterator = snapshotScanner.iterator();
            Iterator<Result> backfilledIterator = backfilledScanner.iterator();
            
            MergeIterator<Result> mergeIt = new MergeIterator<Result>(ResultComparator.INSTANCE, 
                    ImmutableList.of(liveCubeIterator, snapshotIterator, backfilledIterator));
            
            while(mergeIt.hasNext()) {
                // Inform Hadoop that we're still alive. Otherwise it will conclude that something is wrong
                // if map() doesn't return quickly.
                context.progress();
                
                Multimap<Iterator<Result>,Result> results = mergeIt.next();
                ActionRowKeyAndOp actionRowKeyAndOp = makeNewLiveCubeOp(deserializer, 
                        results.get(liveCubeIterator), results.get(snapshotIterator), 
                        results.get(backfilledIterator), context);

                switch(actionRowKeyAndOp.action) {
                case OVERWRITE:
                    byte[] serializedBytes = actionRowKeyAndOp.op.serialize();
                    if(log.isDebugEnabled()) {
                        log.debug("Putting new value " + Hex.encodeHexString(serializedBytes) + " at row " +
                                Hex.encodeHexString(actionRowKeyAndOp.rowKey));
                    }
                    Put put = new Put(actionRowKeyAndOp.rowKey);
                    put.add(cf, HBaseDbHarness.QUALIFIER, serializedBytes);
                    liveCubeHTable.put(put);
                    context.getCounter(Ctrs.ACTION_OVERWRITTEN).increment(1);
                    break;
                case DELETE:
                    if(log.isDebugEnabled()) {
                        log.debug("Deleting row " + Hex.encodeHexString(actionRowKeyAndOp.rowKey));
                    }
                    Delete delete = new Delete(actionRowKeyAndOp.rowKey);
                    liveCubeHTable.delete(delete);
                    context.getCounter(Ctrs.ACTION_DELETED).increment(1);
                    break;
                case LEAVE_ALONE:
                    context.getCounter(Ctrs.ACTION_UNCHANGED).increment(1);
                    break;
                default:
                    throw new RuntimeException("Unknown action");
                }
            }
        } finally {
            if(liveCubeScanner != null) {
                liveCubeScanner.close();
            }
            if(snapshotScanner != null) {
                snapshotScanner.close();
            }
            if(backfilledScanner != null) {
                backfilledScanner.close();
            }
            
            if(liveCubeHTable != null) {
                liveCubeHTable.close();
            }
            if(snapshotHTable != null) {
                snapshotHTable.close();
            }
            if(backfilledHTable != null) {
                backfilledHTable.close();
            }
        }
    }
    
    /**
     * Get the deserializer class name from the job config, instantiate it, and return the instance.
     * @throws RuntimeException if something goes wrong.
     */
    @SuppressWarnings("unchecked")
    private static Deserializer<?> getDeserializer(Configuration conf) {
        String deserializerClassName = conf.get(HBaseBackfillMerger.CONFKEY_DESERIALIZER);
        if(deserializerClassName == null) {
            throw new RuntimeException("Configuration didn't set " + deserializerClassName);
        }
        try {
            Class<?> deserializerClass = Class.forName(deserializerClassName);
            
            if(!Deserializer.class.isAssignableFrom(deserializerClass)) {
                final String errMsg = "The provided deserializer class " + 
                        conf.get(HBaseBackfillMerger.CONFKEY_DESERIALIZER) + "doesn't implement " + 
                        Deserializer.class.getName(); 
                log.error(errMsg);
            }
        
            return ((Class<? extends Deserializer<?>>)deserializerClass).newInstance();
        } catch (Exception e) {
            log.error("Couldn't instantiate deserializer", e);
            throw new RuntimeException(e);
        }
    }
    
    /**
     * Given a multimap returned by the merge iterator, return an Op that should be the new value in the
     * live production cube, or null if the value in the live production cube should be deleted.
     * 
     * @return an ActionRowKeyAndOp telling what action should be taken for this row
     */
    private static final ActionRowKeyAndOp makeNewLiveCubeOp(Deserializer<?> deserializer, Collection<Result> liveCubeResults, 
            Collection<Result> snapshotResults, Collection<Result> backfilledResults, Context ctx) 
            throws IOException {

        Op liveCubeOp = null;
        Op snapshotOp = null;
        Op backfilledOp = null;
        
        byte[] rowKey = null;
        if(!liveCubeResults.isEmpty()) {
            Result result = liveCubeResults.iterator().next();
            liveCubeOp = deserializer.fromBytes(result.value());
            rowKey = result.getRow();
        }
        if(!snapshotResults.isEmpty()) {
            Result result = snapshotResults.iterator().next();
            snapshotOp = deserializer.fromBytes(result.value());
            rowKey = result.getRow();
        }
        if(!backfilledResults.isEmpty()) {
            Result result = backfilledResults.iterator().next();
            backfilledOp = deserializer.fromBytes(result.value());
            rowKey = result.getRow();
        }
        /*
         * Merge the live cube table, the snapshot table, and the backfill table. We assume that the
         * snapshot table contains the values that existing before the backfill began, which means
         * that we can estimate the values that arrived since the snapshot by (live-snapshot). By
         * adding the recently-arrived values to the backfilled values, we solve the problem of data
         * arriving during the snapshot that might not otherwise have been counted.
         * 
         * The following if-else statements enumerate all 8 possibilities of presence/absence of
         * snapshot row, backfill row, and livecube row.
         */
        
        // Case: snapshot exists, backfill exists, liveCube exists
        // If live == snap:
        //    new value is backfill
        // Else:
        //    new value is (live-snap) + backfill
        if(snapshotOp != null && backfilledOp != null && liveCubeOp != null) {
            DebugHack.log("HBaseBackfillMergeMapper 1");
            if(liveCubeOp.equals(snapshotOp)) {
                return new ActionRowKeyAndOp(Action.OVERWRITE, rowKey, backfilledOp);
            }
            Op newLiveCubeValue = (liveCubeOp.subtract(snapshotOp)).add(backfilledOp);
            if(newLiveCubeValue.equals(liveCubeOp)) {
                return new ActionRowKeyAndOp(Action.LEAVE_ALONE, rowKey, null);
            } else {
                return new ActionRowKeyAndOp(Action.OVERWRITE, rowKey, newLiveCubeValue);
            }
        }

        // Case: snapshot exists, backfill empty, liveCube exists
        // If live == snap:
        //    no ops occurred during snapshot, delete row
        // Else
        //       New value is (live-snap)
        else if(snapshotOp != null && backfilledOp == null && liveCubeOp != null) {
            DebugHack.log("HBaseBackfillMergeMapper 2");
            if(liveCubeOp.equals(snapshotOp)) {
                DebugHack.log("HBaseBackfillMergeMapper 2.1");
                return new ActionRowKeyAndOp(Action.DELETE, rowKey, null);
            } else {
                DebugHack.log("HBaseBackfillMergeMapper 2.2");
                Op newLiveCubeValue = liveCubeOp.subtract(snapshotOp);
                return new ActionRowKeyAndOp(Action.OVERWRITE, rowKey, newLiveCubeValue);
            }
        }

        // Case: snapshot empty, backfill exists, liveCube exists
        // New value is backfill + live
        else if(snapshotOp == null && backfilledOp != null && liveCubeOp != null) {
            DebugHack.log("HBaseBackfillMergeMapper 3");
            Op newLiveCubeValue = backfilledOp.add(liveCubeOp);
            return new ActionRowKeyAndOp(Action.OVERWRITE, rowKey, newLiveCubeValue);
        }
        
        // Case: snapshot empty, backfill exists, liveCube empty
        // New value is backfill
        else if(snapshotOp == null && backfilledOp != null && liveCubeOp == null) {
            DebugHack.log("HBaseBackfillMergeMapper 4");
            return new ActionRowKeyAndOp(Action.OVERWRITE, rowKey, backfilledOp);
        }
        
        // Case: snapshot empty, backfill empty, liveCube exists
        // Leave alone
        else if(snapshotOp == null && backfilledOp == null && liveCubeOp != null) {
            DebugHack.log("HBaseBackfillMergeMapper 5");
            return new ActionRowKeyAndOp(Action.LEAVE_ALONE, rowKey, null);
        }
        
        // Case: snapshot empty, backfill empty, liveCube empty
        // No such case, we won't be called, merge iterator doesn't return nonexistent rows
        else if (snapshotOp == null && backfilledOp == null && liveCubeOp == null) {
            throw new RuntimeException("This shouldn't happen, at least one of the ops must be " +
                    "non-null");
        }
        
        // Case: snapshot exists, backfill exists, liveCube empty
        // Error, row should be in live cube if it's in the snapshot
        else if (snapshotOp != null && backfilledOp != null && liveCubeOp == null) {
            throw new RuntimeException("Row shouldn't have disappeared from live cube during " +
            		"snapshotting, something weird is going on. (case 1)");
        }
        
        // Case: snapshot exists, backfill empty, liveCube empty
        // Error, row should be in live cube if it's in the snapshot
        else {
            throw new RuntimeException("Row shouldn't have disappeared from live cube during " +
                    "snapshotting, something weird is going on. (case 2)");
        }
    }
    
    private static enum Action {OVERWRITE, LEAVE_ALONE, DELETE};
    
    private static class ActionRowKeyAndOp {
        public final Action action;
        public final byte[] rowKey;
        public final Op op;

        public ActionRowKeyAndOp(Action action, byte[] rowKey, Op op) {
            this.action = action;
            this.rowKey = rowKey;
            this.op = op;
        }
    }
}