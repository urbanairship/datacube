/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.bucketers.BigEndianLongBucketer;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.ops.SerializableOp;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.NotImplementedException;
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
    private static final byte[] tableName = "myTable".getBytes();
    private static final byte[] cfName = "myCf".getBytes();

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
        final Dimension<Long> dimension = new Dimension<Long>("mydimension", new BigEndianLongBucketer(),
                false, 8);
        List<Dimension<?>> dimensions = ImmutableList.<Dimension<?>>of(dimension);

        Rollup rollup = new Rollup(dimension);
        List<Rollup> rollups = ImmutableList.of(rollup);

        IdService idService = new MapIdService();

        HTablePool pool = new HTablePool(getTestUtil().getConfiguration(), Integer.MAX_VALUE);
        DbHarness<BytesOp> dbHarness = new HBaseDbHarness<BytesOp>(pool, ArrayUtils.EMPTY_BYTE_ARRAY,
                tableName, cfName,  new BytesOpDeserializer(), idService, CommitType.OVERWRITE);

        final DataCube<BytesOp> dataCube = new DataCube<BytesOp>(dimensions, rollups);
        final DataCubeIo<BytesOp> dataCubeIo = new DataCubeIo<BytesOp>(dataCube, dbHarness, 5,
                Long.MAX_VALUE, SyncLevel.FULL_SYNC);

        // Write the value "1" at address "100"
        dataCubeIo.writeSync(new BytesOp(1L), new WriteBuilder(dataCube).at(dimension, 100L));

        // Overwrite it with the value "2"
        dataCubeIo.writeSync(new BytesOp(2L), new WriteBuilder(dataCube).at(dimension, 100L));

        // The value at address "100" should be 2 (and not 1+2 or anything else)
        long cellVal = dataCubeIo.get(new ReadBuilder(dataCube).at(dimension, 100L)).get().getLong();
        Assert.assertEquals(2L, cellVal);
    }

    private static class BytesOp implements SerializableOp {
        public final byte[] bytes;

        public BytesOp(long l) {
            this.bytes = Bytes.toBytes(l);
        }

        @Override
        public byte[] serialize() {
            return bytes;
        }

        @Override
        public SerializableOp deserialize(byte[] serObj) {
            throw new NotImplementedException("Not needed in test");
        }

        @Override
        public Op add(Op otherOp) {
            long otherAsLong = Bytes.toLong(((BytesOp)otherOp).bytes);
            long thisAsLong = Bytes.toLong(this.bytes);
            long added = thisAsLong + otherAsLong;

            return new BytesOp(added);
        }

        @Override
        public Op subtract(Op otherOp) {
            long otherAsLong = Bytes.toLong(((BytesOp)otherOp).bytes);
            long thisAsLong = Bytes.toLong(this.bytes);
            long subtracted = thisAsLong - otherAsLong;

            return new BytesOp(subtracted);
        }

        public long getLong() {
            return Bytes.toLong(bytes);
        }

		@Override
		public Op inverse() {
			// TODO Auto-generated method stub
			return null;
		}
    }

    private static class BytesOpDeserializer implements Deserializer<BytesOp> {
        @Override
        public BytesOp fromBytes(byte[] bytes) {
            return new BytesOp(Bytes.toLong(bytes));
        }
    }
}
