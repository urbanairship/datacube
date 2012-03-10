package com.urbanairship.datacube;

import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

import com.google.common.collect.Maps;

public class UnexplodedAddress {
    private final Map<Dimension,byte[]> coordinates;
    
    /**
     * For internal use only. Obtain an instance of this class by calling
     * {@link DataCube#newAddress()}.
     */
    public UnexplodedAddress() {
        this.coordinates = Maps.newHashMap();
    }
    
    public void at(Dimension dimension, byte[] val) {
        coordinates.put(dimension, val);
    }
    
    /**
     * @return null if this address has no coordinate for the given dimension 
     */
    public byte[] get(Dimension dimension) {
        return coordinates.get(dimension);
    }
    
    /**
     * For internal use only.
     */
    Map<Dimension,byte[]> getValues() {
        return coordinates;
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        boolean firstIteration = true;
        for(Entry<Dimension,byte[]> e : coordinates.entrySet()) {
            if(!firstIteration) {
                sb.append(", ");
            }
            firstIteration = false;
            sb.append(e.getKey().getName());
            sb.append("=");
            sb.append(Arrays.toString(e.getValue()));
        }
        sb.append(")");
        return sb.toString();
    }
    
}
