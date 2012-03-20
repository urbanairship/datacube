package com.urbanairship.datacube.backfill;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.HFileOutputFormat;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

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
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    
    public void runWithCheckedExceptions() throws IOException {
        HTable destHTable = null;
        try {
            Job job = new Job(conf);
            destHTable = new HTable(conf, destTableName);
            
            job.setJobName("DataCube HBase snapshotter");
            job.setMapOutputKeyClass(ImmutableBytesWritable.class);
            job.setMapOutputValueClass(KeyValue.class);
            HFileOutputFormat.configureIncrementalLoad(job, destHTable);
            HFileOutputFormat.setOutputPath(job, hfileOutputPath);
            
            Scan scan = new Scan();
            scan.setCaching(10000);
            scan.addFamily(cf);
            
            TableMapReduceUtil.initTableMapperJob(new String(sourceTableName), scan,
                    ResultToKvsMapper.class, ImmutableBytesWritable.class, KeyValue.class, 
                    job);
     
            job.getConfiguration().set("mapred.map.tasks.speculative.execution", "false");
            job.getConfiguration().set("mapred.reduce.tasks.speculative.execution", "false");
            
            log.debug("Starting HBase mapreduce snapshotter");
            job.waitForCompletion(true);

            log.debug("Starting HBase bulkloader to load snapshot from HFiles");
            new LoadIncrementalHFiles(conf).doBulkLoad(hfileOutputPath, destHTable);
            
            // Delete the mapreduce output directory if it's empty. This will prevent future
            // mapreduce jobs from quitting with "target directory already exists".
            FileSystem fs = FileSystem.get(hfileOutputPath.toUri(), conf);
            FileStatus stat = fs.getFileStatus(hfileOutputPath);
            FileStatus[] dirListing = fs.listStatus(hfileOutputPath); 
            if(stat.isDir() && dirListing.length == 0) {
                fs.delete(hfileOutputPath, true);
            } else {
                log.warn("Mapreduce output dir is non-empty, won't delete: " + 
                        hfileOutputPath);
            }
        } catch (ClassNotFoundException e) { // Mapreduce throws this
            throw new RuntimeException(e);
        } catch (InterruptedException e) { // Mapreduce throws this
            throw new RuntimeException(e);
        } finally {
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
