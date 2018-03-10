package com.urbanairship.datacube.idservicecompat;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.urbanairship.datacube.EmbeddedClusterTestAbstract;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.Util;
import com.urbanairship.datacube.idservices.HBaseIdService;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class TestCompatibility extends EmbeddedClusterTestAbstract {

    //Parent class cleanup methods will nuke table contents between tests.
    private static final byte[] ID_COUNT_TABLE = "ID_COUNTER" .getBytes();
    private static final byte[] ID_LOOKUP_TABLE = "ID_LOOKUP" .getBytes();
    private static final byte[] ID_CF = "L" .getBytes();
    private static final byte[] CUBE_NAME = "CU" .getBytes();
    private static final int DIMENSION = 0;
    private static final int NUM_ID_BYTES = 7;

    private static HBaseTestingUtility testUtil;
    private static HTable lookupTable;
    private static HTable countTable;

    private HBaseIdService newHBaseIdService;
    private OldHBaseIdService oldHBaseIdService;

    @BeforeClass
    public static void makeTables() throws Exception {
        testUtil = getTestUtil();
        countTable = testUtil.createTable(ID_COUNT_TABLE, ID_CF);
        lookupTable = testUtil.createTable(ID_LOOKUP_TABLE, ID_CF);
    }

    @Before
    public void makeIdServices() throws Exception {
        Configuration conf = testUtil.getConfiguration();
        this.oldHBaseIdService = new OldHBaseIdService(conf, ID_LOOKUP_TABLE, ID_COUNT_TABLE, ID_CF, CUBE_NAME);
        this.newHBaseIdService = new HBaseIdService(conf, ID_LOOKUP_TABLE, ID_COUNT_TABLE, ID_CF, CUBE_NAME);
    }

    @Test
    /**
     * Verify that creating IDs with the old ID service then cleanly
     * cutting over to the new doesn't cause mapping issues
     */
    public void testSerial() throws Exception {
        final int mappingsToCreate = 100;
        final Map<String, Long> oldMappings = createNMappings(mappingsToCreate, oldHBaseIdService);
        final Map<String, Long> newMappings = createNMappings(mappingsToCreate, newHBaseIdService);

        //Make sure we mapped values don't intersect
        final Sets.SetView<Long> valueIntersection =
                Sets.intersection(Sets.newHashSet(oldMappings.values()), Sets.newHashSet(newMappings.values()));
        Assert.assertTrue(valueIntersection.isEmpty());

        //Should have a row per mapping, and a single row in the count table;
        Assert.assertEquals(mappingsToCreate * 2, testUtil.countRows(lookupTable));
        Assert.assertEquals(1, testUtil.countRows(countTable));

        //Make sure looking up values created by one service returns the already created mapping
        for (Map.Entry<String, Long> mapping : oldMappings.entrySet()) {
            final byte[] newValue = newHBaseIdService.getOrCreateId(DIMENSION, mapping.getKey().getBytes(), NUM_ID_BYTES);
            Assert.assertEquals(mapping.getValue().longValue(), Util.bytesToLongPad(newValue));
        }

        for (Map.Entry<String, Long> mapping : newMappings.entrySet()) {
            final byte[] oldValue = oldHBaseIdService.getOrCreateId(DIMENSION, mapping.getKey().getBytes(), NUM_ID_BYTES);
            Assert.assertEquals(mapping.getValue().longValue(), Util.bytesToLongPad(oldValue));
        }

        //No new rows should have been created
        Assert.assertEquals(mappingsToCreate * 2, testUtil.countRows(lookupTable));
        Assert.assertEquals(1, testUtil.countRows(countTable));

    }

    private static Map<String, Long> createNMappings(int numMappings, IdService idService) throws Exception {
        final ImmutableMap.Builder<String, Long> mappings = ImmutableMap.builder();
        for (int i = 0; i < numMappings; i++) {
            String id = RandomStringUtils.randomAlphanumeric(10);
            final byte[] mapped = idService.getOrCreateId(DIMENSION, id.getBytes(), NUM_ID_BYTES);
            mappings.put(id, Util.bytesToLongPad(mapped));
        }
        return mappings.build();
    }

    @Test
    @Ignore //This reliably times out, ignore it for now.
    /**
     * Verify that creating mappings via the old and new method simultaneously doesn't cause mapping issues.
     * This test is very sensitive to thread starvation, if it fails try again.
     */
    public void testConcurrent() throws Exception {
        final int instancesPerIdService = 1;
        final int numIdsToMap = 10000;
        final ExecutorService executor = Executors.newFixedThreadPool(instancesPerIdService * 2);

        final List<String> idsToMap = Lists.newArrayListWithCapacity(numIdsToMap);
        for (int i = 0; i < numIdsToMap; i++) {
            idsToMap.add(i, RandomStringUtils.randomAlphanumeric(20));
        }

        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch startedLatch = new CountDownLatch(instancesPerIdService * 2);
        ImmutableList.Builder<Future<Map<String, Long>>> futures = ImmutableList.builder();
        for (int i = 0; i < instancesPerIdService; i++) {
            futures.add(executor.submit(new MappingCreator(oldHBaseIdService, idsToMap, startLatch, startedLatch)));
            futures.add(executor.submit(new MappingCreator(newHBaseIdService, idsToMap, startLatch, startedLatch)));
        }

        //And they're off!
        startLatch.countDown();
        startedLatch.await();

        ImmutableList.Builder<Map<String, Long>> resultBuilder = ImmutableList.builder();
        for (Future<Map<String, Long>> future : futures.build()) {
            resultBuilder.add(future.get(120, TimeUnit.SECONDS));
        }

        //All id maps should be the same.
        final ImmutableList<Map<String, Long>> results = resultBuilder.build();
        for (Map<String, Long> outerResult : results) {
            for (Map<String, Long> innerResult : results) {
                Assert.assertEquals(outerResult, innerResult);
            }
        }

        // Row counts should be exactly the number of mappings and dimensions, respectively
        Assert.assertEquals(numIdsToMap, testUtil.countRows(lookupTable));
        Assert.assertEquals(1, testUtil.countRows(countTable));

        // The counter should be at or above the number of mappings
        final Get get = new Get(oldHBaseIdService.makeCounterKey(DIMENSION));
        get.addFamily(ID_CF);
        final Result result = countTable.get(get);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.isEmpty());

        final byte[] value = result.getValue(ID_CF, OldHBaseIdService.QUALIFIER);
        Assert.assertTrue(Util.bytesToLongPad(value) >= numIdsToMap);
    }

    private static class MappingCreator implements Callable<Map<String, Long>> {
        private final IdService idService;
        private final List<String> idsToMap;
        private final CountDownLatch startLatch;
        private final CountDownLatch startedLatch;

        private MappingCreator(IdService idService, List<String> idsToMap, CountDownLatch startLatch, CountDownLatch startedLatch) {
            this.idService = idService;
            this.idsToMap = idsToMap;
            this.startLatch = startLatch;
            this.startedLatch = startedLatch;
        }

        @Override
        public Map<String, Long> call() throws Exception {
            startLatch.await();
            startedLatch.countDown();
            final ImmutableMap.Builder<String, Long> mappings = ImmutableMap.builder();
            for (String id : idsToMap) {
                final byte[] mapped = idService.getOrCreateId(DIMENSION, id.getBytes(), NUM_ID_BYTES);
                mappings.put(id, Util.bytesToLongPad(mapped));
            }
            return mappings.build();
        }
    }
}
