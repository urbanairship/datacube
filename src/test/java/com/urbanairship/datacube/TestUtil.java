package com.urbanairship.datacube;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.hbase.HBaseTestingUtility;

public class TestUtil {
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
}
