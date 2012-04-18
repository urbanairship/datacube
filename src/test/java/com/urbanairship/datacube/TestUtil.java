package com.urbanairship.datacube;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public abstract class TestUtil {
    public static final String HADOOP_LOG_DIR = "/tmp/datacube_hadoop_logs";
    
    public static void preventMiniClusterNPE(HBaseTestingUtility hbaseTestUtil) {
        // HBaseTestingUtility will NPE unless we set this
        hbaseTestUtil.getConfiguration().set("hadoop.log.dir", HADOOP_LOG_DIR); 
    }
    
    public static void cleanupHadoopLogs() {
        try {
            FileUtils.deleteDirectory(new File(HADOOP_LOG_DIR));
        } catch (IOException e) { }
    }
    
    /**
     * Clear out all tables between tests.
     */
    public static void deleteAllRows(HTable hTable) throws IOException {
        ResultScanner scanner = hTable.getScanner(new Scan());
        for(Result result: scanner) {
            Delete delete = new Delete(result.getRow());
            hTable.delete(delete);
        }
    }
}
