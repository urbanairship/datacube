package com.urbanairship.datacube.idservices;

import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;
import com.urbanairship.datacube.DataCube;
import com.urbanairship.datacube.Dimension;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.dbharnesses.MapDbHarness.BoxedByteArray;


/**
 * An implementation of IdService that's backed by an in-memory map instead of a database.
 * This is useful for testing.
 */
public class MapIdService implements IdService {
    private final Map<BoxedByteArray,byte[]> idMap = Maps.newHashMap();
    private final Map<Dimension<?>,Integer> dimensionIndexes = Maps.newHashMap();
    
    public MapIdService(DataCube<?> cube) {
        List<Dimension<?>> cubeDimensions = cube.getDimensions(); 
        for(int i=0; i<cubeDimensions.size(); i++) {
            dimensionIndexes.put(cubeDimensions.get(i), i);
        }
    }
    
    @Override
    public byte[] getId(Dimension<?> dimension, byte[] bytes, int len) {
        // TODO Auto-generated method stub
        return null;
    }
}
