package com.urbanairship.datacube;

/**
 * Cube oplog mutation type for storing a long counter.
 */
public class LongOp implements Op {
    private final long val;
    public static final LongOpDeserializer DESERIALIZER = new LongOpDeserializer(); 
    
    public LongOp(long val) {
        this.val = val;
    }

    public Op combine(Op otherOp) {
        if(!(otherOp instanceof LongOp)) {
            throw new RuntimeException();
        }
        return new LongOp(val + ((LongOp)otherOp).val);
    }
    
    @Override
    public byte[] serialize() {
        return Util.longToBytes(val);
    }

    public static class LongOpDeserializer implements Deserializer<LongOp> {
        /**
         * Not instantiable, use the singleton in LongOp.DESERIALIZER. It's thread-safe.
         */
        protected LongOpDeserializer() { }
        
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
