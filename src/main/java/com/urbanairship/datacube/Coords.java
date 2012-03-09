package com.urbanairship.datacube;

import java.nio.ByteBuffer;
import java.util.List;

public class Coords {
    private final List<Dimension> dimensions;
    private final byte[][] valuesForDimensions;
    
    /**
     * For internal use only. Obtain an instance of this class by calling
     * {@link DataCube#newCoords()}.
     */
    Coords(List<Dimension> dimensions) {
        this.dimensions = dimensions;
        this.valuesForDimensions = new byte[dimensions.size()][];
    }
    
    /**
     * For internal use only. Obtain an instance of this class by calling
     * {@link DataCube#newCoords()}.
     * @param dimensions
     * @param values
     */
    Coords(List<Dimension> dimensions, byte[][] values) {
       this.dimensions = dimensions;
       this.valuesForDimensions = values;
    }
    
//    public Coords(Coords copyFrom) {
//        this.dimensions = copyFrom.dimensions;
//        this.valuesForDimensions = new byte[copyFrom.size()];
//        for(int i=0; i<copyFrom.valuesForDimensions.size(); i++) {
//            this.valuesForDimensions[i] = Arrays.copyOf(original, newLength)
//        }
//    }
    /**
     * @return this for chaining
     */
    public Coords set(Dimension dimension, byte[] val) {
        int idx = dimensions.indexOf(dimension);
        if(idx == -1) {
            throw new IllegalArgumentException("Dimension doesn't exist in cube");
        }
        valuesForDimensions[idx] = val;
        return this;
    }
    
    public byte[] get(Dimension dim) {
        int idx = dimensions.indexOf(dim);
        if(idx == -1) {
            throw new IllegalArgumentException("No such dimension");
        }
        return valuesForDimensions[idx];
    }
    
    public List<Dimension> getDimensions() {
        return dimensions;
    }
    
    /**
     * For internal use only.
     */
    byte[][] getValues() {
        return valuesForDimensions;
    }
    
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("(");
        for(int i=0; i<dimensions.size(); i++) {
            sb.append(dimensions.get(i).getName());
            sb.append("=");
            sb.append(valuesForDimensions[i].toString());
        }
        sb.append(")");
        return sb.toString();
    }
    
    /**
     * Get a byte array encoding the coordinates of this cell in the Cube. For internal use only.
     * @return
     */
    byte[] toKey(byte[] prefix) {
        int totalKeySize = prefix.length;
        for(Dimension dimension: dimensions) {
            totalKeySize += dimension.getNumFieldBytes();
        }
        ByteBuffer bb = ByteBuffer.allocate(totalKeySize);
        bb.put(prefix);
        for(int i=0; i<valuesForDimensions.length; i++) {
            byte[] dimensionVal = valuesForDimensions[i];
            Dimension dimension = dimensions.get(i);
            if(dimensionVal.length != dimension.getNumFieldBytes()) {
                throw new IllegalArgumentException("Field length was wrong. For dimension " +
                        dimension + ", expected length " + dimension.getNumFieldBytes() + 
                        " but was " + dimensionVal.length);
            }
            bb.put(dimensionVal);
        }
        return bb.array();
    }
}
