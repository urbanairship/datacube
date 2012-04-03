package com.urbanairship.datacube;

import com.urbanairship.datacube.ops.LongOp;


/**
 * A cell mutation or cell bucket. For example, a cube storing counters would contain Ops that 
 * are numbers (see e.g. {@link LongOp}).
 */
public interface Op extends CSerializable {
    /**
     * @return an Op that combines the effect of this and otherOp.
     */
    public Op add(Op otherOp);
    
    /**
     * Return the difference between this op and the given one. 
     * 
     * This should satisfy the following property:
     *   y + x.subtract(y) = x   
     *   
     * This holds for integers, e.g. 10 + (15 - 10) = 15
     *   
     * An example using LongOp: 
     *   assert new LongOp(10).add(new LongOp(15).subtract(10).equals(15)
     *   
     */
    public Op subtract(Op otherOp);
    
    /**
     * Subclasses must override equals() and hashCode().
     */
    @Override
    public boolean equals(Object other); 
    @Override
    public int hashCode(); 
}
