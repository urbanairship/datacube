package com.urbanairship.datacube.backfill;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Takes a "snapshot" of an HBase column family in two steps:
 *  - Mapreduce to write all KeyValues of the source into HFiles on disk
 *  - Use LoadIncrementalHFiles to bulk-load the HFiles into the target CF.
 * 
 * The snapshot isn't a true snapshot because writers could alter the source data
 * while we're mapreducing over it.
 */
public class HBaseSnapshotter implements Runnable {
    private static final Logger log = LogManager.getLogger(HBaseSnapshotter.class);
    
    private final byte[] sourceTableName;
    private final byte[] destTableName;
    private final byte[] cf;
    private final Configuration conf;
    private final Path hfileOutputPath;

    public HBaseSnapshotter(Configuration conf, byte[] sourceTable, byte[] cf,
            byte[] destTable, Path hfileOutputPath) {
        this.sourceTableName = sourceTable;
        this.destTableName = destTable;
        this.conf = conf;
        this.hfileOutputPath = hfileOutputPath;
        this.cf = cf;
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
        ResultScanner destScanner = null;
        try {
            Job job = new Job(conf);
            destHTable = new HTable(conf, destTableName);
            
            destScanner = destHTable.getScanner(cf);
            if(destScanner.iterator().hasNext()) {
                destScanner.close();
                log.error("Snapshotter won't run because destination CF isn't empty");
                return false;
            }
            destScanner.close();
            
            job.setJobName("DataCube HBase snapshotter");
            HFileOutputFormat.configureIncrementalLoad(job, destHTable);
            HFileOutputFormat.setOutputPath(job, hfileOutputPath);
            
            Scan scan = new Scan();
            scan.setCaching(5000);
            scan.addFamily(cf);
            
            TableMapReduceUtil.initTableMapperJob(new String(sourceTableName), scan,
                    ResultToKvsMapper.class, ImmutableBytesWritable.class, KeyValue.class, 
                    job);
     
            job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");
            job.getConfiguration().set("mapred.reduce.tasks.speculative.execution", "false");
            
            log.debug("Starting HBase mapreduce snapshotter");
            if(!job.waitForCompletion(true)) {
                log.error("Job return false, mapreduce must have failed");
                return false;
            }

            log.debug("Starting HBase bulkloader to load snapshot from HFiles");
            new LoadIncrementalHFiles(conf).doBulkLoad(hfileOutputPath, destHTable);
            
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
            destScanner.close();
            
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
        }
    }
        
    
    public static class ResultToKvsMapper extends TableMapper<ImmutableBytesWritable,KeyValue> {
        @Override
        protected void map(ImmutableBytesWritable key, Result result,
                Context context) throws IOException,
                InterruptedException {
            for(KeyValue kv: result.list()) {
                context.write(key, kv);
            }
        }
    }
}
