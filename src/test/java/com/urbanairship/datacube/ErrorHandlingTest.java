package com.urbanairship.datacube;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.HTablePool;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.bucketers.StringToBytesBucketer;
import com.urbanairship.datacube.dbharnesses.HBaseDbHarness;
import com.urbanairship.datacube.idservices.CachingIdService;
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.ops.LongOp;

/**
 * Make sure database errors are handled
 */
public class ErrorHandlingTest extends EmbeddedClusterTestAbstract {
    private static Logger log = LoggerFactory.getLogger(ErrorHandlingTest.class);
    
    @Test
    public void test() throws Exception {
        log.info("You can ignore exceptions and scary stack traces from this test");
        IdService idService = new CachingIdService(5, new MapIdService());

        Configuration conf = getTestUtil().getConfiguration();
        HTablePool pool = new HTablePool(conf, Integer.MAX_VALUE);
        
        DbHarness<LongOp> dbHarness = new HBaseDbHarness<LongOp>(pool, "XY".getBytes(), 
                "nonexistentTable".getBytes(), "nonexistentCf".getBytes(), LongOp.DESERIALIZER, 
                idService, CommitType.INCREMENT, 5, 2, 2, null);
        
        DataCube<LongOp> cube;
        
        Dimension<String> zipcode = new Dimension<String>("zipcode", new StringToBytesBucketer(), 
                true, 5);

        Rollup zipRollup = new Rollup(zipcode);
        
        List<Dimension<?>> dimensions =  ImmutableList.<Dimension<?>>of(zipcode);
        List<Rollup> rollups = ImmutableList.of(zipRollup);
        
        cube = new DataCube<LongOp>(dimensions, rollups);

        DataCubeIo<LongOp> dataCubeIo = new DataCubeIo<LongOp>(cube, dbHarness, 1, Long.MAX_VALUE, 
                SyncLevel.BATCH_ASYNC);
        
        dataCubeIo.writeAsync(new LongOp(1), new WriteBuilder(cube)
            .at(zipcode, "97212"));

        dataCubeIo.writeAsync(new LongOp(1), new WriteBuilder(cube)
            .at(zipcode, "97212"));

        dataCubeIo.flush();

        try {
            dataCubeIo.writeAsync(new LongOp(1), new WriteBuilder(cube)
                .at(zipcode, "97212"));
            Assert.fail("Cube should not have accepted more writes after an error!");
        } catch (AsyncException e) {
            // This exception *should* happen. Because we wrote to a nonexistent table.
            Assert.assertTrue(e.getCause().getCause().getCause() instanceof TableNotFoundException);
        }
    }
}
