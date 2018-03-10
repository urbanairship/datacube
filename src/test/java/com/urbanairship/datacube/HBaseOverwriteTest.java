/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Longs;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.bucketers.BigEndianLongBucketer;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.idservices.MapIdService;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

/**
 * Check that {@link CommitType#OVERWRITE} works.
 */
public class HBaseOverwriteTest extends EmbeddedClusterTestAbstract {
    private static final byte[] tableName = "myTable" .getBytes();
    private static final byte[] cfName = "myCf" .getBytes();

    @BeforeClass
    public static void init() throws Exception {
        getTestUtil().createTable(tableName, cfName).close();
    }

    /**
     * Have a bunch of threads competing to update the same row using checkAndPut operations and
     * assert that all the end value is what we expect.
     */
    @Test
    public void test() throws Exception {
        final Dimension<Long, Long> dimension = new Dimension<Long, Long>("mydimension", new BigEndianLongBucketer(),
                false, 8);
        List<Dimension<?, ?>> dimensions = ImmutableList.of(dimension);

        Rollup rollup = new Rollup(dimension);
        List<Rollup> rollups = ImmutableList.of(rollup);

        IdService idService = new MapIdService();

        HTablePool pool = new HTablePool(getTestUtil().getConfiguration(), Integer.MAX_VALUE);
        DbHarness<BytesOp> dbHarness = new HBaseDbHarness<BytesOp>(pool, ArrayUtils.EMPTY_BYTE_ARRAY,
                tableName, cfName, new BytesOpDeserializer(), idService, CommitType.OVERWRITE);

        final DataCube<BytesOp> dataCube = new DataCube<BytesOp>(dimensions, rollups);
        final DataCubeIo<BytesOp> dataCubeIo = new DataCubeIo<BytesOp>(dataCube, dbHarness, 5,
                Long.MAX_VALUE, SyncLevel.FULL_SYNC);

        // Write the value "1" at address "100"
        dataCubeIo.writeSync(new BytesOp(Longs.toByteArray(1L)), new WriteBuilder().at(dimension, 100L));

        // Overwrite it with the value "2"
        dataCubeIo.writeSync(new BytesOp(Longs.toByteArray(2L)), new WriteBuilder().at(dimension, 100L));

        // The value at address "100" should be 2 (and not 1+2 or anything else)
        long cellVal = Longs.fromByteArray(dataCubeIo.get(new ReadBuilder(dataCube).at(dimension, 100L)).get().get());
        Assert.assertEquals(2L, cellVal);
    }

    @Test
    public void testSetGet() throws Exception {
        final long value = 100L;
        final Dimension<Long, Long> dimension = new Dimension<>("mydimension", new BigEndianLongBucketer(),
                false, 8);
        List<Dimension<?, ?>> dimensions = ImmutableList.of(dimension);

        Rollup rollup = new Rollup(dimension);
        List<Rollup> rollups = ImmutableList.of(rollup);

        IdService idService = new MapIdService();

        HTablePool pool = new HTablePool(getTestUtil().getConfiguration(), Integer.MAX_VALUE);
        DbHarness<BytesOp> dbHarness = new HBaseDbHarness<BytesOp>(pool, ArrayUtils.EMPTY_BYTE_ARRAY,
                tableName, cfName, new BytesOpDeserializer(), idService, CommitType.INCREMENT);

        final DataCube<BytesOp> dataCube = new DataCube<BytesOp>(dimensions, rollups);

        Address address = Address.create(dataCube);
        address.at(dimension, Bytes.toBytes(999L));

        dbHarness.set(address, new BytesOp(Longs.toByteArray(value)));
        Assert.assertEquals(Longs.fromByteArray(new BytesOp(Longs.toByteArray(value)).get()), Longs.fromByteArray(dbHarness.get(address).get().get()));
    }

}
