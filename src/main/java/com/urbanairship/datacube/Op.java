package com.urbanairship.datacube;


/**
 * A cell mutation or cell coord. For example, a cube storing counters would contain Ops that are
 * numbers.
 */
public interface Op {
    /**
     * @return an Op that combines the effect of this and otherOp.
     */
    public Op combine(Op otherOp);
    
    public byte[] serialize();
}
