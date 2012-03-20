package com.urbanairship.datacube;

import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.idservices.HBaseIdService;
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.ops.LongOp;

public class HBaseTest {
    public static final byte[] CUBE_DATA_TABLE = "cube_data".getBytes();
    public static final byte[] IDSERVICE_LOOKUP_TABLE = "idservice_data".getBytes();
    public static final byte[] IDSERVICE_COUNTER_TABLE = "idservice_counter".getBytes();
    
    public static final byte[] CF = "c".getBytes();
    
    private static HBaseTestingUtility hbaseTestUtil = new HBaseTestingUtility();
    
    @BeforeClass
    public static void setupCluster() throws Exception {
        hbaseTestUtil.startMiniCluster();
        hbaseTestUtil.createTable(CUBE_DATA_TABLE, CF);
        hbaseTestUtil.createTable(IDSERVICE_COUNTER_TABLE, CF);
        hbaseTestUtil.createTable(IDSERVICE_LOOKUP_TABLE, CF);
    }
    
    @AfterClass
    public static void teardownCluster() throws Exception {
        hbaseTestUtil.shutdownMiniCluster();
    }
    
    @Test
    public void hbaseForCubeDataTest() throws Exception {
        IdService idService = new MapIdService();
        
        DbHarness<LongOp> hbaseDbHarness = new HBaseDbHarness<LongOp>(
                hbaseTestUtil.getConfiguration(), "hbaseForCubeDataTest".getBytes(), 
                CUBE_DATA_TABLE, CF, LongOp.DESERIALIZER, idService);
        
        DbHarnessTests.basicTest(hbaseDbHarness);
    }
    
    @Test
    public void basicIdServiceTest() throws Exception {
        IdService idService = new HBaseIdService(hbaseTestUtil.getConfiguration(),
                IDSERVICE_LOOKUP_TABLE, IDSERVICE_COUNTER_TABLE, CF, 
                "basicIdServiceTest".getBytes());
        
        IdServiceTests.basicTest(idService);
    }

    @Test
    public void exhaustionIdServiceTest() throws Exception {
        for(int i=1; i<2; i++) {
            IdService idService = new HBaseIdService(hbaseTestUtil.getConfiguration(),
                    IDSERVICE_LOOKUP_TABLE, IDSERVICE_COUNTER_TABLE, CF, 
                    "exhaustionIdServiceTest".getBytes());
            IdServiceTests.testExhaustion(idService, i, i);
        }
    }
}
