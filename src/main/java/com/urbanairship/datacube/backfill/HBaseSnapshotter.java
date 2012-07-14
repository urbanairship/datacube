/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.backfill;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.hfile.Compression.Algorithm;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.regionserver.StoreFile.BloomType;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Takes a "snapshot" of an HBase column family in two steps:
 *  - Mapreduce to write all KeyValues of the source into HFiles on disk
 *  - Use LoadIncrementalHFiles to bulk-load the HFiles into the target CF.
 * 
 * The snapshot isn't a true snapshot because writers could alter the source data
 * while we're mapreducing over it.
 */
public class HBaseSnapshotter implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(HBaseSnapshotter.class);
    
    private final byte[] sourceTableName;
    private final byte[] destTableName;
    private final byte[] cf;
    private final Configuration conf;
    private final Path hfileOutputPath;
    private final boolean okIfTableExists;
    private final byte[] startKey;
    private final byte[] stopKey;

    /**
     * @param okIfTableExists if the destination table already exists, and this bool is false,
     * then there will be a TableExistsException.
     * @param startKey if non-null, the HBase scan will use this key as its start row
     * @param stopKey if non-null, the HBase scan will use this key as its stop row 
     */
    public HBaseSnapshotter(Configuration conf, byte[] sourceTable, byte[] cf,
            byte[] destTable, Path hfileOutputPath, boolean okIfTableExists, byte[] startKey,
            byte[] stopKey) {
        this.sourceTableName = sourceTable;
        this.destTableName = destTable;
        this.conf = conf;
        this.hfileOutputPath = hfileOutputPath;
        this.cf = cf;
        this.okIfTableExists = okIfTableExists;
        this.startKey = startKey;
        this.stopKey = stopKey;
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
        HTable destHTable = null;
        HTable sourceHTable = null;
        ResultScanner destScanner = null;
        try {
            Job job = new Job(conf);
            
            sourceHTable = new HTable(conf, sourceTableName);
            Pair<byte[][],byte[][]> regionStartsEnds = sourceHTable.getStartEndKeys();
            
            HBaseAdmin admin = new HBaseAdmin(conf);
            if(admin.tableExists(destTableName)) {
                if(!okIfTableExists) {
                    throw new TableExistsException(new String(destTableName) + " already exists");
                }
            } else {
                createSnapshotTable(conf, destTableName, 
                        BackfillUtil.getSplitKeys(regionStartsEnds), cf);                
            }
           
            destHTable = new HTable(conf, destTableName);

            Scan scan = new Scan();
            scan.setCaching(5000);
            scan.addFamily(cf);
            if(startKey != null) {
                scan.setStartRow(startKey);
            }
            if(stopKey != null) {
                scan.setStopRow(stopKey);
            }
            
            TableMapReduceUtil.initTableMapperJob(new String(sourceTableName), scan,
                    ResultToKvsMapper.class, ImmutableBytesWritable.class, KeyValue.class, 
                    job);

            job.setJobName("DataCube HBase snapshotter");
            job.setJarByClass(HBaseSnapshotter.class);
            HFileOutputFormat.configureIncrementalLoad(job, destHTable);
            HFileOutputFormat.setOutputPath(job, hfileOutputPath);
            
            job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");
            job.getConfiguration().set("mapred.reduce.tasks.speculative.execution", "false");
            
            log.debug("Starting HBase mapreduce snapshotter");
            if(!job.waitForCompletion(true)) {
                log.error("Job return false, mapreduce must have failed");
                return false;
            }

            log.debug("Starting HBase bulkloader to load snapshot from HFiles");
            try {
            	new LoadIncrementalHFiles(conf).doBulkLoad(hfileOutputPath, destHTable);
            } catch (Exception e) {
            	throw new IOException("Bulkloader couldn't run", e);
            }
            
            // Delete the mapreduce output directory if it's empty. This will prevent future
            // mapreduce jobs from quitting with "target directory already exists".
            FileSystem fs = FileSystem.get(hfileOutputPath.toUri(), conf);
            FileStatus stat = fs.getFileStatus(hfileOutputPath);
            FileStatus[] dirListing = fs.listStatus(hfileOutputPath); 
            if(stat.isDir() && dirListing.length <= 3) {
                // In the case where bulkloading was successful, there should be three remaining
                // entries in the HFile directory: _SUCCESS, _LOGS, and cfname. Go ahead and delete.
                fs.delete(hfileOutputPath, true);
            } else {
                List<String> fileNames = new ArrayList<String>();
                for(FileStatus dentry: dirListing) {
                    fileNames.add(dentry.getPath().toString());
                }
                final String errMsg = "Mapreduce output dir had unexpected contents, won't delete: " + 
                        hfileOutputPath + " contains " + fileNames; 
                log.error(errMsg);
                throw new RuntimeException(errMsg);
            }
            
            
            destScanner = destHTable.getScanner(cf);
            if(!destScanner.iterator().hasNext()) {
                log.warn("Destination CF was empty after snapshotting");
            }
            return true;
        } catch (ClassNotFoundException e) { // Mapreduce throws this
            throw new RuntimeException(e);
        } finally {
            if(destScanner != null) {
                destScanner.close();
            }
            if (destHTable != null) {
                destHTable.close();
            }
            if(sourceHTable != null) {
                sourceHTable.close();
            }
        }
    }
    
    private static void createSnapshotTable(Configuration conf, byte[] tableName, byte[][] splitKeys,
            byte[] cf) throws IOException {
        HBaseAdmin hba = new HBaseAdmin(conf);
        HColumnDescriptor cfDesc = new HColumnDescriptor(cf);
        cfDesc.setBloomFilterType(BloomType.NONE);
        cfDesc.setMaxVersions(1);
        cfDesc.setCompressionType(Algorithm.NONE); // TODO change to snappy in 0.92
        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        tableDesc.addFamily(cfDesc);
        hba.createTable(tableDesc, splitKeys);
    }   
    
    public static class ResultToKvsMapper extends TableMapper<ImmutableBytesWritable,KeyValue> {
        @Override
        protected void map(ImmutableBytesWritable key, Result result,
                Context context) throws IOException, InterruptedException {
//            DebugHack.log("Snapshot mapper running");
            for(KeyValue kv: result.list()) {
                context.write(key, kv);
            }
        }
    }
    
    
}
