/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Classes that use the embedded HBase/MapReduce test cluster should inherit this class.
 */
@Ignore
public class EmbeddedClusterTestAbstract {
    private static final Logger log = LoggerFactory.getLogger(EmbeddedClusterTestAbstract.class);
    
    public static final String HADOOP_LOG_DIR = "/tmp/datacube_hadoop_logs";
    
    private static HBaseTestingUtility hbaseTestUtil = null;
    
    protected synchronized static HBaseTestingUtility getTestUtil() throws Exception {
        if(hbaseTestUtil == null) {
            hbaseTestUtil = new HBaseTestingUtility();

            // Workaround for HBASE-5711, we need to set config value dfs.datanode.data.dir.perm
            // equal to the permissions of the temp dirs on the filesystem. These temp dirs were
            // probably created using this process' umask. So we guess the temp dir permissions as
            // 0777 & ~umask, and use that to set the config value.
            try {
                Process process = Runtime.getRuntime().exec("/bin/sh -c umask");
                BufferedReader br = new BufferedReader(new InputStreamReader(process.getInputStream()));
                int rc = process.waitFor();
                if(rc == 0) {
                    String umask = br.readLine();
    
                    int umaskBits = Integer.parseInt(umask, 8);
                    int permBits = 0777 & ~umaskBits;
                    String perms = Integer.toString(permBits, 8);
                    
                    log.info("Setting dfs.datanode.data.dir.perm to " + perms);
                    hbaseTestUtil.getConfiguration().set("dfs.datanode.data.dir.perm", perms);
                } else {
                    log.warn("Failed running umask command in a shell, nonzero return value");
                }
            } catch (Exception e) {
                // ignore errors, we might not be running on POSIX, or "sh" might not be on the path
                log.warn("Couldn't get umask", e);
            }
            
            hbaseTestUtil.startMiniCluster();
            
             // HBaseTestingUtility will NPE unless we set this
            hbaseTestUtil.getConfiguration().set("hadoop.log.dir", HADOOP_LOG_DIR);
            
            hbaseTestUtil.startMiniMapReduceCluster();
        }

        return hbaseTestUtil;
    }
    
    /**
     * Remove all tables between test classes.
     */
    @AfterClass
    public static void deleteAllTables() throws Exception {
        HTableDescriptor[] tableDescs = getTestUtil().getHBaseAdmin().listTables();
        HBaseAdmin admin = getTestUtil().getHBaseAdmin();
        
        for(HTableDescriptor tableDesc: tableDescs) {
            admin.disableTable(tableDesc.getName());
            admin.deleteTable(tableDesc.getName());
        }
    }

    /**
     * Remove all rows between tests in the same class.
     */
    @After
    public void deleteAllRows() throws Exception {
        HTableDescriptor[] tableDescs = getTestUtil().getHBaseAdmin().listTables();
        
        for(HTableDescriptor tableDesc: tableDescs) {
            HTable hTable = null;
            ResultScanner rs = null;
            
            try {
                hTable = new HTable(getTestUtil().getConfiguration(), tableDesc.getName());
                rs = hTable.getScanner(new Scan());
                
                for(Result result: rs) {
                    hTable.delete(new Delete(result.getRow()));
                }
            } finally {
                if(rs != null) {
                    rs.close();
                }
                if(hTable != null) {
                    hTable.close();
                }
            }
        }
    }
    
    public static void deleteAllRows(HTable hTable) throws IOException {
        ResultScanner scanner = hTable.getScanner(new Scan());
        for(Result result: scanner) {
            Delete delete = new Delete(result.getRow());
            hTable.delete(delete);
        }
    }
}
