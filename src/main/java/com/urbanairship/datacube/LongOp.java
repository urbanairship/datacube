package com.urbanairship.datacube;

/**
 * Cube oplog mutation type for storing a long counter.
 */
public class LongOp implements Op {
    private final long val;
    
    public LongOp(long val) {
        this.val = val;
    }

    public Op combine(Op otherOp) {
        if(!(otherOp instanceof LongOp)) {
            throw new RuntimeException();
        }
        return new LongOp(val + ((LongOp)otherOp).val);
    }
    
//    @Override
//    public Optional<Op> attemptCombine(Op otherOp) {
//        return Optional.absent();
//    }

    @Override
    public byte[] serialize() {
        return Util.longToBytes(val);
    }

    public static class LongOpDeserializer implements Deserializer<LongOp> {
        @Override
        public LongOp fromBytes(byte[] bytes) {
            return new LongOp(Util.bytesToLong(bytes));
        }
    }
    
    public String toString() {
        return Long.toString(val);
    }
    
    public long getValue() {
        return val;
    }
}
