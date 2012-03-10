package com.urbanairship.datacube;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

import com.google.common.base.Optional;

/**
 * For testing, this is is a backing store for a cube that lives in memory. Saves us from calling a
 * DB just to test the cube logic.
 */
public class MapDbHarness<T extends Op> implements DbHarness<T> {
    private final List<Dimension> dimensions;
    private final Map<BoxedByteArray,byte[]> map;
    private final Deserializer<T> deserializer;
    
    public MapDbHarness(List<Dimension> dimensions, Map<BoxedByteArray,byte[]> map, 
            Deserializer<T> deserializer) {
        this.dimensions = dimensions;
        this.map = map;
        this.deserializer = deserializer;
    }
    
    @Override
    public void runBatch(Batch<T> batch) throws IOException {
        
        for(Map.Entry<ExplodedAddress,T> entry: batch.getMap().entrySet()) {
            ExplodedAddress address = entry.getKey();
            T opFromBatch = entry.getValue();
            
            Optional<T> existingOpInDb = get(address);
            T newValForDb;
            
            if(existingOpInDb.isPresent()) {
                newValForDb = (T) ((existingOpInDb.get()).combine(opFromBatch));
            } else {
                newValForDb = opFromBatch;
            }
            
            
            byte[] mapKey = address.toKey(dimensions);
            map.put(new BoxedByteArray(mapKey), newValForDb.serialize());
        }
    }

    @Override
    public Optional<T> get(ExplodedAddress address) throws IOException {
        byte[] mapKey = address.toKey(dimensions);
        byte[] bytes = map.get(new BoxedByteArray(mapKey));
        if(bytes == null) {
            return Optional.absent();
        }
        
        return Optional.of(deserializer.fromBytes(bytes));
    }

    @Override
    public List<Optional<T>> multiGet(List<ExplodedAddress> addresses) throws IOException {
        throw new NotImplementedException();
    }
    
    public static class BoxedByteArray {
        private final byte[] bytes;
        
        public BoxedByteArray(byte[] bytes) {
            this.bytes = bytes;
        }
        
        public int hashCode() {
            return Arrays.hashCode(bytes);
        }
        
        @Override
        public boolean equals(Object o) {
            return Arrays.equals(bytes, ((BoxedByteArray)o).bytes);
        }
    }

}
