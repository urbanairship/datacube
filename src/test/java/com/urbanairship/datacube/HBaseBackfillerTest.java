package com.urbanairship.datacube;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;
import com.urbanairship.datacube.backfill.HBaseBackfillMerger;
import com.urbanairship.datacube.backfill.HBaseSnapshotter;
import com.urbanairship.datacube.bucketers.StringToBytesBucketer;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.idservices.HBaseIdService;
import com.urbanairship.datacube.ops.LongOp;

public class HBaseBackfillerTest {
    private static HBaseTestingUtility hbaseTestUtil;
    
    public static final byte[] CUBE_DATA_TABLE = "cube_data".getBytes();
    public static final byte[] SNAPSHOT_DEST_TABLE = "snapshot".getBytes();
    public static final byte[] BACKFILLED_TABLE = "backfilled".getBytes();
    public static final byte[] IDSERVICE_LOOKUP_TABLE = "idservice_data".getBytes();
    public static final byte[] IDSERVICE_COUNTER_TABLE = "idservice_counter".getBytes();
    public static final byte[] CF = "c".getBytes();
    
    private static HTable cubeHTable = null;
    private static HTable snapshotHTable = null;
    private static HTable backfilledHTable = null;
    private static HTable idServiceLookupHTable = null;
    private static HTable idServiceCounterHTable = null;
    
    @BeforeClass
    public static void setupCluster() throws Exception {
        // HBaseTestingUtility will NPE unless we set this
        Configuration conf = new Configuration();
        conf.set("hadoop.log.dir", "/tmp/test_logs");

        hbaseTestUtil = new HBaseTestingUtility(conf);
        
        hbaseTestUtil.startMiniCluster();
        hbaseTestUtil.startMiniMapReduceCluster();
        cubeHTable = hbaseTestUtil.createTable(CUBE_DATA_TABLE, CF);
        snapshotHTable = hbaseTestUtil.createTable(SNAPSHOT_DEST_TABLE, CF);
        backfilledHTable = hbaseTestUtil.createTable(BACKFILLED_TABLE, CF);
        idServiceLookupHTable = hbaseTestUtil.createTable(IDSERVICE_COUNTER_TABLE, CF);
        idServiceCounterHTable = hbaseTestUtil.createTable(IDSERVICE_LOOKUP_TABLE, CF);
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        hbaseTestUtil.shutdownMiniMapReduceCluster();
        hbaseTestUtil.shutdownMiniCluster();
    }
    
    /**
     * Backfilling shouldn't change any live values if the backfilled counts are the same as the live
     * counts.
     */
    @Test
    public void testBackfillIdempotence() throws Exception {
        Configuration conf = hbaseTestUtil.getConfiguration();
        
        IdService idService = new HBaseIdService(conf, IDSERVICE_LOOKUP_TABLE, 
                IDSERVICE_COUNTER_TABLE, CF, ArrayUtils.EMPTY_BYTE_ARRAY);
        DbHarness<LongOp> hbaseDbHarness = new HBaseDbHarness<LongOp>(conf, 
                ArrayUtils.EMPTY_BYTE_ARRAY, CUBE_DATA_TABLE, CF, LongOp.DESERIALIZER, 
                idService);
        
        // Get some cube data into the source table, doesn't really matter what.
        DbHarnessTests.basicTest(hbaseDbHarness);
        
        // Snapshot the source table
        Assert.assertTrue(new HBaseSnapshotter(conf, CUBE_DATA_TABLE, CF, SNAPSHOT_DEST_TABLE,
                new Path("hdfs:///test_hfiles")).runWithCheckedExceptions());
        // The snapshot should be equal to the source table
        assertTablesEqual(conf, cubeHTable, snapshotHTable);
        
        // Simulate a backfill by copying the live cube
        Assert.assertTrue(new HBaseSnapshotter(conf, CUBE_DATA_TABLE, CF, BACKFILLED_TABLE,
                new Path("hdfs:///test_hfiles")).runWithCheckedExceptions());
        
        // Since the backfilled table is identical to the snapshot, there should be no changes to the
        // live production table
        HBaseBackfillMerger backfiller = new HBaseBackfillMerger(conf, CUBE_DATA_TABLE, SNAPSHOT_DEST_TABLE, 
                BACKFILLED_TABLE, CF, LongOp.LongOpDeserializer.class);
        Assert.assertTrue(backfiller.runWithCheckedExceptions());
        assertTablesEqual(conf, snapshotHTable, cubeHTable);
        
    }

    /**
     * Backfilling 0 events should lead to an empty live table
     */
    @Test
    public void testBackfillingWithEmpty() throws Exception {
        Configuration conf = hbaseTestUtil.getConfiguration();
        
        IdService idService = new HBaseIdService(conf, IDSERVICE_LOOKUP_TABLE, 
                IDSERVICE_COUNTER_TABLE, CF, ArrayUtils.EMPTY_BYTE_ARRAY);
        DbHarness<LongOp> hbaseDbHarness = new HBaseDbHarness<LongOp>(conf, 
                ArrayUtils.EMPTY_BYTE_ARRAY, CUBE_DATA_TABLE, CF, LongOp.DESERIALIZER, 
                idService);
        
        // Get some cube data into the source table, doesn't really matter what.
        DbHarnessTests.basicTest(hbaseDbHarness);
        
        // Copy the source table using the snapshotter
        Assert.assertTrue(new HBaseSnapshotter(conf, CUBE_DATA_TABLE, CF, SNAPSHOT_DEST_TABLE,
                new Path("hdfs:///test_hfiles")).runWithCheckedExceptions());
        
        // The snapshot should be equal to the source table
        assertTablesEqual(conf, cubeHTable, snapshotHTable);
        
        HBaseBackfillMerger backfiller = new HBaseBackfillMerger(conf, CUBE_DATA_TABLE, SNAPSHOT_DEST_TABLE, 
                BACKFILLED_TABLE, CF, LongOp.LongOpDeserializer.class);
        Assert.assertTrue(backfiller.runWithCheckedExceptions());
        
        // After backfilling from an empty "backfilled" table, the live cube should have no rows
        Assert.assertTrue(!new HTable(conf, CUBE_DATA_TABLE).getScanner(CF).iterator().hasNext());
    }
    
    /**
     * Clear out all tables between tests.
     */
    @After
    public void deleteAllRows() throws IOException {
        List<HTable> hTables = ImmutableList.of(cubeHTable, snapshotHTable, backfilledHTable, 
                idServiceLookupHTable, idServiceCounterHTable);
        for(HTable hTable: hTables) {
            ResultScanner scanner = hTable.getScanner(CF);
            for(Result result: scanner) {
                Delete delete = new Delete(result.getRow());
                delete.deleteFamily(CF);
                hTable.delete(delete);
            }
        }
    }
    
    @Test
    public void testMutationsWhileBackfilling() throws Exception {
        Configuration conf = hbaseTestUtil.getConfiguration();
        
        IdService idService = new HBaseIdService(conf, IDSERVICE_LOOKUP_TABLE, 
                IDSERVICE_COUNTER_TABLE, CF, ArrayUtils.EMPTY_BYTE_ARRAY);
        DbHarness<LongOp> hbaseDbHarness = new HBaseDbHarness<LongOp>(conf, 
                ArrayUtils.EMPTY_BYTE_ARRAY, CUBE_DATA_TABLE, CF, LongOp.DESERIALIZER, 
                idService);
        
        Dimension<String> onlyDimension = new Dimension<String>("mydimension", 
                new StringToBytesBucketer(), true, 2);
        
        Rollup rollup = new Rollup(onlyDimension, BucketType.IDENTITY);
        List<Dimension<?>> dims = ImmutableList.<Dimension<?>>of(onlyDimension);
        List<Rollup> rollups = ImmutableList.of(rollup);
        DataCube<LongOp> cube = new DataCube<LongOp>(dims, rollups);
        DataCubeIo<LongOp> cubeIo = new DataCubeIo<LongOp>(cube, hbaseDbHarness, 1);
        
        // Before doing any snapshotting/backfilling, there's one value "5" in the cube.
        cubeIo.write(new LongOp(5), new WriteBuilder(cube).at(onlyDimension, "coord1"));
        
        // Snapshot the source table
        Assert.assertTrue(new HBaseSnapshotter(conf, CUBE_DATA_TABLE, CF, SNAPSHOT_DEST_TABLE,
                new Path("hdfs:///test_hfiles")).runWithCheckedExceptions());

        // Simulate a backfill by copying the live cube
        Assert.assertTrue(new HBaseSnapshotter(conf, CUBE_DATA_TABLE, CF, BACKFILLED_TABLE,
                new Path("hdfs:///test_hfiles")).runWithCheckedExceptions());
        
        // Simulate two writes to the live table that wouldn't be seen by the app as it backfills.
        // This is like a client doing a write concurrently with a backfill.
        cubeIo.write(new LongOp(6), new WriteBuilder(cube).at(onlyDimension, "coord1"));
        cubeIo.write(new LongOp(7), new WriteBuilder(cube).at(onlyDimension, "coord2"));
        
        HBaseBackfillMerger backfiller = new HBaseBackfillMerger(conf, CUBE_DATA_TABLE, SNAPSHOT_DEST_TABLE, 
                BACKFILLED_TABLE, CF, LongOp.LongOpDeserializer.class);
        Assert.assertTrue(backfiller.runWithCheckedExceptions());
        
        // After everyhing's done, the two writes that occurred concurrently with the backfill
        // should be seen in the live table. coord1 should be 5+6=11 and coord2 should be 7.
        Assert.assertEquals(11L, 
                cubeIo.get(new ReadAddressBuilder(cube).at(onlyDimension, "coord1")).get().getValue());
        Assert.assertEquals(7L, 
                cubeIo.get(new ReadAddressBuilder(cube).at(onlyDimension, "coord2")).get().getValue());
    }

    public static void assertTablesEqual(Configuration conf, HTable leftTable, HTable rightTable) 
            throws Exception {
        
        ResultScanner leftRs = leftTable.getScanner(CF);
        ResultScanner rightRs = rightTable.getScanner(CF);
        Iterator<Result> leftIterator = leftRs.iterator();
        Iterator<Result> rightIterator = rightRs.iterator();
        
        MergeIterator<Result> mergeIt = new MergeIterator<Result>(ResultComparator.INSTANCE,
                ImmutableList.of(leftIterator, rightIterator));
        
        while(mergeIt.hasNext()) {
            Multimap<Iterator<Result>,Result> results = mergeIt.next();
            
            List<KeyValue> leftKvs = results.get(leftIterator).iterator().next().list();
            List<KeyValue> rightKvs = results.get(rightIterator).iterator().next().list();
            
            Assert.assertEquals(leftKvs.size(), rightKvs.size());
            for(int i=0; i<leftKvs.size(); i++) {
                KeyValue leftKv = leftKvs.get(i);
                KeyValue rightKv = rightKvs.get(i);
                
                // Compare every field except timestamp
                Assert.assertArrayEquals(leftKv.getFamily(), rightKv.getFamily());
                Assert.assertArrayEquals(leftKv.getQualifier(), rightKv.getQualifier());
                Assert.assertArrayEquals(leftKv.getValue(), rightKv.getValue());
            }
        }
    }
}
