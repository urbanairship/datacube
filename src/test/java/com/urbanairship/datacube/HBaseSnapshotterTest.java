package com.urbanairship.datacube;

import java.util.Arrays;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.urbanairship.datacube.backfill.HBaseSnapshotter;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.idservices.HBaseIdService;
import com.urbanairship.datacube.ops.LongOp;

public class HBaseSnapshotterTest {
    private static HBaseTestingUtility hbaseTestUtil;
    
    public static final byte[] CUBE_DATA_TABLE = "cube_data".getBytes();
    public static final byte[] SNAPSHOT_DEST_TABLE = "snapshot".getBytes();
    public static final byte[] IDSERVICE_LOOKUP_TABLE = "idservice_data".getBytes();
    public static final byte[] IDSERVICE_COUNTER_TABLE = "idservice_counter".getBytes();
    public static final byte[] CF = "c".getBytes();
    
    @BeforeClass
    public static void setupCluster() throws Exception {
        // HBaseTestingUtility will NPE unless we set this
        Configuration conf = new Configuration();
        conf.set("hadoop.log.dir", "/tmp/test_logs");

        hbaseTestUtil = new HBaseTestingUtility(conf);
        
        hbaseTestUtil.startMiniCluster();
        hbaseTestUtil.startMiniMapReduceCluster();
        hbaseTestUtil.createTable(CUBE_DATA_TABLE, CF);
        hbaseTestUtil.createTable(SNAPSHOT_DEST_TABLE, CF);
        hbaseTestUtil.createTable(IDSERVICE_COUNTER_TABLE, CF);
        hbaseTestUtil.createTable(IDSERVICE_LOOKUP_TABLE, CF);
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        hbaseTestUtil.shutdownMiniMapReduceCluster();
        hbaseTestUtil.shutdownMiniCluster();
    }
    
    @Test
    public void test() throws Exception {
        Configuration conf = hbaseTestUtil.getConfiguration();
        
        IdService idService = new HBaseIdService(conf, IDSERVICE_LOOKUP_TABLE, 
                IDSERVICE_COUNTER_TABLE, CF, ArrayUtils.EMPTY_BYTE_ARRAY);
        DbHarness<LongOp> hbaseDbHarness = new HBaseDbHarness<LongOp>(conf, 
                ArrayUtils.EMPTY_BYTE_ARRAY, CUBE_DATA_TABLE, CF, LongOp.DESERIALIZER, 
                idService);
        
        // Get some cube data into the source table, doesn't really matter what.
        DbHarnessTests.basicTest(hbaseDbHarness);
        
        // Copy the source table using the snapshotter
        new HBaseSnapshotter(conf, CUBE_DATA_TABLE, CF, SNAPSHOT_DEST_TABLE,
                new Path("hdfs:///test_hfiles")).runWithCheckedExceptions();
        
        // The snapshot should be equal to the source table
        assertTablesEqual(conf, CUBE_DATA_TABLE, SNAPSHOT_DEST_TABLE);
    }
    
    public static void assertTablesEqual(Configuration conf, byte[] leftTableName, byte[] rightTableName) 
            throws Exception {
        HTable leftTable = new HTable(conf, leftTableName);
        HTable rightTable = new HTable(conf, rightTableName);
        
        ResultScanner leftRs = leftTable.getScanner(CF);
        ResultScanner rightRs = rightTable.getScanner(CF);
        
        for(Result leftResult: leftRs) {
            Result rightResult = rightRs.next();
            System.err.println("leftResult=" + leftResult + " rightResult=" + rightResult);

            Assert.assertNotNull(rightResult);
            Assert.assertTrue(!rightResult.isEmpty());

            DataOutputBuffer leftBuffer = new DataOutputBuffer();
            DataOutputBuffer rightBuffer = new DataOutputBuffer();
            leftResult.write(leftBuffer);
            rightResult.write(rightBuffer);
            byte[] leftBytes = Arrays.copyOf(leftBuffer.getData(), leftBuffer.getLength());
            byte[] rightBytes = Arrays.copyOf(rightBuffer.getData(), rightBuffer.getLength());
            
            Assert.assertArrayEquals(leftBytes, rightBytes);
        }
        
        leftRs.close();
        rightRs.close();
        
        leftTable.close();
        rightTable.close();
    }
}
