package com.urbanairship.datacube;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;

import com.google.common.base.Optional;

public class DataCube<T extends Op> {
//    private final String name;
    private final List<Dimension> dims;
    private final Optional<List<Aggregate>> aggregates;
    
    /**
     * Use this constructor when you don't aggregate counters over multiple dimensions.
     */
    public DataCube(List<Dimension> dims) {
        this.dims = dims;
        this.aggregates = Optional.absent();
    }
    
    public DataCube(String name, List<Dimension> dims, List<Aggregate> aggregates) {
        this.dims = dims;
        this.aggregates = Optional.of(aggregates);
    }
    
    public Batch<T> getBatch(Coords cubeCoords, T op) {
        
        // TODO Bucketize into potentially multiple buckets
        
        // TODO add aggregate cells
        
        if(aggregates.isPresent()) {
            throw new NotImplementedException();
        }
        
        Map<Coords,T> newBatchMap = new HashMap<Coords,T>(); //ArrayListMultimap.create();
        newBatchMap.put(cubeCoords, op);
        return new Batch<T>(newBatchMap);
    }
    
    public Coords newCoords() {
        return new Coords(dims);
    }
}
