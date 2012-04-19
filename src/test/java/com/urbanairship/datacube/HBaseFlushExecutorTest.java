package com.urbanairship.datacube;

import java.util.List;

import junit.framework.Assert;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.bucketers.BigEndianLongBucketer;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.idservices.MapIdService;

/**
 * Test flushing of batches at {@link SyncLevel#BATCH_ASYNC} and {@link SyncLevel#BATCH_SYNC}.
 */
public class HBaseFlushExecutorTest extends EmbeddedClusterTestAbstract {
    private static final byte[] tableName = "myTable".getBytes();
    private static final byte[] cfName = "myCf".getBytes();
    
    @BeforeClass
    public static void init() throws Exception {
        getTestUtil().createTable(tableName, cfName).close();
    }
    
    @Test
    public void testBatchSync() throws Exception {
        doTestForSyncLevel(SyncLevel.BATCH_ASYNC);
        
    }
    
    @Test
    public void testBatchAsync() throws Exception {
        doTestForSyncLevel(SyncLevel.BATCH_SYNC);
    }
    
    public void doTestForSyncLevel(SyncLevel syncLevel) throws Exception {
        final Dimension<Long> dimension = new Dimension<Long>("mydimension", new BigEndianLongBucketer(),
                false, 8);
        List<Dimension<?>> dimensions = ImmutableList.<Dimension<?>>of(dimension);
        
        Rollup rollup = new Rollup(dimension);
        List<Rollup> rollups = ImmutableList.of(rollup);
        
        IdService idService = new MapIdService();
        
        HTablePool pool = new HTablePool(getTestUtil().getConfiguration(), Integer.MAX_VALUE);
        DbHarness<BytesOp> dbHarness = new HBaseDbHarness<BytesOp>(pool, ArrayUtils.EMPTY_BYTE_ARRAY, 
                tableName, cfName,  new BytesOpDeserializer(), idService, CommitType.READ_COMBINE_CAS);
        
        final DataCube<BytesOp> dataCube = new DataCube<BytesOp>(dimensions, rollups);
        final DataCubeIo<BytesOp> dataCubeIo = new DataCubeIo<BytesOp>(dataCube, dbHarness, 10, 
                Long.MAX_VALUE, syncLevel);
        
        for(int i=0; i<1000; i++) {
            WriteBuilder at = new WriteBuilder(dataCube).at(dimension, (long)i);
            dataCubeIo.writeAsync(new BytesOp(i), at);
        }
        
        dataCubeIo.flush();
        
        for(int i=0; i<1000; i++) {
            ReadBuilder at = new ReadBuilder(dataCube).at(dimension, (long)i);
            Optional<BytesOp> optVal = dataCubeIo.get(at);
            Assert.assertTrue(optVal.isPresent());
            Assert.assertEquals(i, Bytes.toLong(optVal.get().bytes));
        }
    }
    
    private static class BytesOp implements Op {
        public final byte[] bytes;
        
        public BytesOp(long l) {
            this.bytes = Bytes.toBytes(l);
        }

        @Override
        public byte[] serialize() {
            return bytes;
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
    }
    
    private static class BytesOpDeserializer implements Deserializer<BytesOp> {
        @Override
        public BytesOp fromBytes(byte[] bytes) {
            return new BytesOp(Bytes.toLong(bytes));
        }
    }
}
