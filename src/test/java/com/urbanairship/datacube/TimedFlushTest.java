package com.urbanairship.datacube;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.bucketers.EnumToOrdinalBucketer;
import com.urbanairship.datacube.dbharnesses.MapDbHarness;
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.ops.LongOp;

/**
 * Test that timed flushing of batches works.
 */
public class TimedFlushTest {
    
    enum Color {RED, BLUE};
    
    @Test
    public void test() throws Exception {
        Dimension<Color> colorDimension = new Dimension<Color>("color", new EnumToOrdinalBucketer<Color>(1), false, 1);
        Rollup colorRollup = new Rollup(colorDimension);
        IdService idService = new MapIdService();
        ConcurrentMap<BoxedByteArray,byte[]> backingMap = Maps.newConcurrentMap();
        DbHarness<LongOp> dbHarness = new MapDbHarness<LongOp>(backingMap, 
                new LongOp.LongOpDeserializer(), CommitType.READ_COMBINE_CAS, 3, idService);
        
        DataCube<LongOp> cube = new DataCube<LongOp>(ImmutableList.<Dimension<?>>of(colorDimension), 
                ImmutableList.of(colorRollup));
        DataCubeIo<LongOp> cubeIo = new DataCubeIo<LongOp>(cube, dbHarness, Integer.MAX_VALUE,
                TimeUnit.SECONDS.toMillis(1));
        
        // Immediately after the first write, the write should be hanging out in the batch and not yet 
        // written to the backing dbHarness.
        cubeIo.write(new LongOp(1), new WriteBuilder(cube).at(colorDimension, Color.RED));
        Assert.assertFalse(cubeIo.get(new ReadAddressBuilder(cube).at(colorDimension, Color.RED)).isPresent());
        
        // If we wait one second for the batch timeout to expire and write again, both writes should
        // be flushed to the backing dbHarness.
        Thread.sleep(1001);
        cubeIo.write(new LongOp(1), new WriteBuilder(cube).at(colorDimension, Color.RED));
        Assert.assertEquals(2, 
                cubeIo.get(new ReadAddressBuilder(cube).at(colorDimension, Color.RED)).get().getLong());
        
        // If we do another write, it should not be flushed to the database since it's part of a new
        // batch whose timeout has not yet expired.
        cubeIo.write(new LongOp(1), new WriteBuilder(cube).at(colorDimension, Color.RED));
        Assert.assertEquals(2, 
                cubeIo.get(new ReadAddressBuilder(cube).at(colorDimension, Color.RED)).get().getLong());
        
    }
}
