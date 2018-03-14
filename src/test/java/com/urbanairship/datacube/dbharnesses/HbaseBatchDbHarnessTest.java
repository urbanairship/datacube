package com.urbanairship.datacube.dbharnesses;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.DataCube;
import com.urbanairship.datacube.DbHarness;
import com.urbanairship.datacube.Dimension;
import com.urbanairship.datacube.EmbeddedClusterTestAbstract;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.ReadBuilder;
import com.urbanairship.datacube.Rollup;
import com.urbanairship.datacube.WriteBuilder;
import com.urbanairship.datacube.bucketers.BigEndianLongBucketer;
import com.urbanairship.datacube.idservices.HBaseIdService;
import com.urbanairship.datacube.ops.LongOp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;


public class HbaseBatchDbHarnessTest extends EmbeddedClusterTestAbstract {
    final private static byte[] table = "testing".getBytes();
    final private static byte[] cube = "cube".getBytes();
    final private static byte[] id = "id".getBytes();
    final private static byte[] counter = "counter".getBytes();
    final private static byte[] cf = "c".getBytes();
    private static IdService idService;
    private static HTablePool pool;
    private static DataCube<LongOp> dataCube;
    private static Dimension<Long> dim1;
    private static Dimension<Long> dim2;

    @org.junit.BeforeClass
    public static void setUp() throws Exception {
        getTestUtil().createTable(table, cf).close();
        getTestUtil().createTable(id, cf).close();
        getTestUtil().createTable(counter, cf).close();

    }

    private static final Logger log = LogManager.getLogger(HbaseBatchDbHarnessTest.class);

    @Test
    public void matchesHbaseDbHarness() throws Exception {
        Configuration conf = getTestUtil().getConfiguration();
        idService = new HBaseIdService(conf, id, counter, cf, cube);
        pool = new HTablePool(conf, Integer.MAX_VALUE);
        dim1 = new Dimension<Long>("1", new BigEndianLongBucketer(),
                true, 2);
        dim2 = new Dimension<Long>("2", new BigEndianLongBucketer(),
                false, 8);

        List<Dimension<?>> dimensions = ImmutableList.<Dimension<?>>of(dim1, dim2);

        dataCube = new DataCube<LongOp>(dimensions, ImmutableList.of(new Rollup(dim1, dim2), new Rollup(dim1), new Rollup(dim2)));
        HbaseBatchDbHarness hot = new HbaseBatchDbHarness(cube, table, cf, "testing", idService, pool);
        HBaseDbHarness<LongOp> old = new HBaseDbHarness<LongOp>(pool, cube, table, cf, LongOp.DESERIALIZER, idService, DbHarness.CommitType.INCREMENT);

        WriteBuilder writeBuilder = new WriteBuilder();
        writeBuilder.at(dim1, 1l);
        writeBuilder.at(dim2, 2l);

        CompletableFuture wrote = new CompletableFuture();

        Batch<LongOp> writes = dataCube.getWrites(writeBuilder, new LongOp(1));
        old.runBatchAsync(writes, new AfterExecute<LongOp>() {
            @Override
            public void afterExecute(Throwable t) {
                if (t != null) {
                    wrote.completeExceptionally(t);
                } else {
                    wrote.complete(null);
                }
            }
        });
        old.flush();
        log.info("waiting for old write");
        wrote.join();
        log.info("finished writing old write");

        Map<Address, LongOp> map = writes.getMap();
        log.info("writing new " + map);
        hot.increment(map, attempt -> {
            if (attempt > 0) {
                return false;
            }
            return true;
        });
        log.info("finished writing new");

        List<Optional<LongOp>> optionals = old.multiGet(ImmutableList.of(
                new ReadBuilder(dataCube).at(dim1, 1L).build(),
                new ReadBuilder(dataCube).at(dim2, 2L).build(),
                new ReadBuilder(dataCube).at(dim1, 1L).at(dim2, 2L).build()
        ));

        List<Long> collect = optionals.stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .map(LongOp::getLong)
                .collect(Collectors.toList());

        assertEquals(collect, ImmutableList.of(2L, 2L, 2L));


    }


    @org.junit.After
    public void tearDown() throws Exception {
    }
}