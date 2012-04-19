package com.urbanairship.datacube;

import java.io.IOException;

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

/**
 * Classes that use the embedded HBase/MapReduce test cluster should inherit this class.
 */
@Ignore
public class EmbeddedClusterTestAbstract {
    public static final String HADOOP_LOG_DIR = "/tmp/datacube_hadoop_logs";
    
    private static HBaseTestingUtility hbaseTestUtil = null;
    
    protected synchronized static HBaseTestingUtility getTestUtil() throws Exception {
        if(hbaseTestUtil == null) {
            hbaseTestUtil = new HBaseTestingUtility();
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
