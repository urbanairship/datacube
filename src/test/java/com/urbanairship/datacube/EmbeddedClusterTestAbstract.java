/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
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
        	// Run on some arbitrary ports to avoid conflicting with existing daemons
        	Configuration testConf = new Configuration();
        	testConf.set("fs.default.name", "hdfs://localhost:38247");
        	testConf.set("hbase.master.port", "38248");
        	testConf.set("hbase.master.info.port", "38249");
        	testConf.set("hbase.regionserver.port", "38250");
        	testConf.set("hbase.regionserver.info.port", "38251");
        	testConf.set("hbase.zookeeper.property.clientPort", "38252");
        	testConf.set("hbase.zookeeper.peerport", "38253");
        	testConf.set("hbase.zookeeper.leaderport", "38254");
        	testConf.set("hbase.rest.port", "38255");
        	testConf.set("dfs.datanode.http.address", "38256");
        	testConf.set("dfs.datanode.ipc.address", "38257");
        	testConf.set("dfs.namenode.http-address", "38258");
        	
            hbaseTestUtil = new HBaseTestingUtility(testConf);
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
