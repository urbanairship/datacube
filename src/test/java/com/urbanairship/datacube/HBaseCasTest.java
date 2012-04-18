package com.urbanairship.datacube;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.bucketers.BigEndianLongBucketer;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.idservices.MapIdService;

/**
 * Test using HBaseDbHarness with {@link CommitType#READ_COMBINE_CAS}.
 * 
 * This test will probably spam warnings to the log about CAS retries, which is normal.
 */
public class HBaseCasTest extends EmbeddedClusterTest {
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
        
        Configuration conf = getTestUtil().getConfiguration();
        DbHarness<BytesOp> dbHarness = new HBaseDbHarness<BytesOp>(conf, ArrayUtils.EMPTY_BYTE_ARRAY, 
                tableName, cfName,  new BytesOpDeserializer(), idService, CommitType.READ_COMBINE_CAS,
                3, 20, 20);
        
        final DataCube<BytesOp> dataCube = new DataCube<BytesOp>(dimensions, rollups);
        final DataCubeIo<BytesOp> dataCubeIo = new DataCubeIo<BytesOp>(dataCube, dbHarness, 5, Long.MAX_VALUE,
                SyncLevel.BATCH_SYNC);
        
        final int numThreads = 10;
        final int updatesPerThread = 10;
        ThreadPoolExecutor executor = new ThreadPoolExecutor(numThreads, numThreads, Long.MAX_VALUE,
                TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        
        
        for(int i=0; i<numThreads; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        for(int j=0; j<updatesPerThread; j++) {
                            WriteBuilder at = new WriteBuilder(dataCube).at(dimension, 1234L);
                            dataCubeIo.writeSync(new BytesOp(Bytes.toBytes(1L)), at);
                        } 
                    } catch (IOException e) {
                        Assert.fail();
                    }
                }
            });
        }
        
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        
        BytesOp cellValue = dataCubeIo.get(new ReadBuilder(dataCube).at(dimension, 1234L)).get();
        long cellValueLong = Bytes.toLong(cellValue.bytes);
        Assert.assertEquals((long)(updatesPerThread * numThreads), cellValueLong);
    }
    
    private static class BytesOp implements Op {
        public final byte[] bytes;
        
        public BytesOp(byte[] bytes) {
            this.bytes = bytes;
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
            
            return new BytesOp(Bytes.toBytes(added));
        }

        @Override
        public Op subtract(Op otherOp) {
            long otherAsLong = Bytes.toLong(((BytesOp)otherOp).bytes);
            long thisAsLong = Bytes.toLong(this.bytes);
            long subtracted = thisAsLong - otherAsLong;
            
            return new BytesOp(Bytes.toBytes(subtracted));
        }
    }
    
    private static class BytesOpDeserializer implements Deserializer<BytesOp> {
        @Override
        public BytesOp fromBytes(byte[] bytes) {
            return new BytesOp(bytes);
        }
    }
}
