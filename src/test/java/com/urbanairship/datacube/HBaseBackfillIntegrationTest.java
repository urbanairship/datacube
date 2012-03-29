package com.urbanairship.datacube;

import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.joda.time.DateTime;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.bucketers.EnumToOrdinalBucketer;
import com.urbanairship.datacube.bucketers.HourDayMonthBucketer;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.idservices.HBaseIdService;
import com.urbanairship.datacube.ops.LongOp;

public class HBaseBackfillIntegrationTest {
    enum DeviceType {ANDROID, APPLE, WINDOWS};
    
    private static final byte[] LIVE_CUBE_TABLE = "live_cube_table".getBytes();
    private static final byte[] SNAPSHOT_TABLE = "snapshot_table".getBytes();
    private static final byte[] BACKFILL_TABLE = "backfill_table".getBytes();
    
    private static final byte[] IDSERVICE_LOOKUP_TABLE = "lookup_table".getBytes();
    private static final byte[] IDSERVICE_COUNTER_TABLE = "counter_table".getBytes();
    private static final byte[] CF = "c".getBytes();
    
    private static HBaseTestingUtility hbaseTestUtil;

    private static final Dimension<DeviceType> deviceDimension = new Dimension<DeviceType>("Device", 
            new EnumToOrdinalBucketer<DeviceType>(1), false, 1);
    private static final Dimension<DateTime> timeDimension = new Dimension<DateTime>("time", 
            new HourDayMonthBucketer(), false, 8);
    
    private static DbHarness<LongOp> dbHarness;   
    private static IdService idService;
    private static DataCubeIo<LongOp> dataCubeIo;
    
    @BeforeClass
    public static void beforeClass() throws Exception {
        hbaseTestUtil = new HBaseTestingUtility();
        hbaseTestUtil.startMiniCluster();
        Configuration conf = hbaseTestUtil.getConfiguration(); 
        idService = new HBaseIdService(conf, IDSERVICE_LOOKUP_TABLE, IDSERVICE_COUNTER_TABLE, CF, 
                ArrayUtils.EMPTY_BYTE_ARRAY);
        dbHarness = new HBaseDbHarness<LongOp>(conf, ArrayUtils.EMPTY_BYTE_ARRAY, LIVE_CUBE_TABLE, 
                CF, LongOp.DESERIALIZER, idService);

        Rollup rollup = new Rollup(deviceDimension, timeDimension);
        
        List<Dimension<?>> dims = ImmutableList.<Dimension<?>>of(deviceDimension, timeDimension);
        List<Rollup> rollups = ImmutableList.of(rollup); 
        
        DataCube<LongOp> cube = new DataCube<LongOp>(dims, rollups);
        
        dataCubeIo = new DataCubeIo<LongOp>(cube, dbHarness, batchSize)
        

    }

    
//    DataCubeIo<LongOp> cubeIo = new DataCubeIo<LongOp>(cube, )
    
    @Test
    public void test() {
        //
        
        
    }
    
    private static void insertInitialEvents(DataCube<LongOp> cube) {
        
    }
}
