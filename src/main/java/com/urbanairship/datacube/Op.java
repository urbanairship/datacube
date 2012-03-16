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
    public Op combine(Op otherOp);
}
