package com.urbanairship.datacube;


/**
 * A cell mutation or cell value. For example, a cube storing counters would contain Ops that are
 * numbers.
 */
public interface Op {
    /**
     * @return an Op that combines the effect of this and otherOp.
     */
    public Op combine(Op otherOp);
    
    public byte[] serialize();
}
