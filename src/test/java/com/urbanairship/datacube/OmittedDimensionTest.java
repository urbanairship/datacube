package com.urbanairship.datacube;

import java.util.List;
import java.util.concurrent.ConcurrentMap;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.urbanairship.datacube.DbHarness.CommitType;
import com.urbanairship.datacube.bucketers.BigEndianLongBucketer;
import com.urbanairship.datacube.dbharnesses.MapDbHarness;
import com.urbanairship.datacube.idservices.CachingIdService;
import com.urbanairship.datacube.idservices.MapIdService;
import com.urbanairship.datacube.ops.LongOp;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class OmittedDimensionTest {

    public static final Dimension<Long> X = new Dimension<Long>("X", new BigEndianLongBucketer(), false, 8, true);
    public static final Dimension<Long> Y = new Dimension<Long>("Y", new BigEndianLongBucketer(), false, 8, false);
    public static final Dimension<Long> Z = new Dimension<Long>("Z", new BigEndianLongBucketer(), false, 8, true);

    @Test
    public void testAddress() throws Exception {
        ConcurrentMap<BoxedByteArray, byte[]> backingMap = Maps.newConcurrentMap();
        IdService idService = new CachingIdService(4, new MapIdService());
        DbHarness<LongOp> dbHarness = new MapDbHarness<LongOp>(backingMap, LongOp.DESERIALIZER, CommitType.OVERWRITE, idService);

        List<Dimension<?>> dims = ImmutableList.<Dimension<?>>of(X, Y, Z);
        List<Rollup> rollups = ImmutableList.of(new Rollup(X, Y), new Rollup(Y, Z));

        DataCube<LongOp> cube = new DataCube<LongOp>(dims, rollups);

        DataCubeIo<LongOp> cubeIo = new DataCubeIo<LongOp>(cube, dbHarness, 1, Long.MAX_VALUE, SyncLevel.FULL_SYNC);

        cubeIo.writeSync(new LongOp(1), new WriteBuilder(cube).at(X, 1L).at(Y, 1L));
        assertEquals(1L, cubeIo.get(new ReadBuilder(cube).at(X, 1L).at(Y, 1L)).get().getLong());

        cubeIo.writeSync(new LongOp(1), new WriteBuilder(cube).at(Y, 1L).at(Z, 1L));
        assertEquals(1L, cubeIo.get(new ReadBuilder(cube).at(Y, 1L).at(Z, 1L)).get().getLong());
    }
}
