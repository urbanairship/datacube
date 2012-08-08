package com.urbanairship.datacube;

/**
 * An operation which is used to execute a given operation
 * on a value specified by K key within a Map-like structure
 */

// TODO: Maybe every Op should be like ColumnOp with a Key?
public class ColumnOp<K> implements Op {

    private Op op;
	private byte[] key;

    /**
     * Constructs a MapOp which performs an increment on key using op
     * @param key key for which to increment the value
     * @param op operation which should perform the increment, depending on the type of the value
     */
    public ColumnOp(byte[] key, Op op) {
        this.key = key;
        this.op = op;
    }

    public byte[] getKey() {
    	return this.key;
    }

    public Op getWrappedOp() {
    	return this.op;
    }

    @Override
    public Op add(Op otherOp) {
        if(!(otherOp instanceof ColumnOp<?>)) {
            throw new RuntimeException("Incompatible Operations");
        }

        ColumnOp otherMapOp = (ColumnOp)otherOp;

        if(getKey() != otherMapOp.getKey()) {
           throw new RuntimeException("Operations are not combinable, since they dont affect the same key");
        }

        Op thisWrapperOp = getWrappedOp();
        Op otherWrappedOp = otherMapOp.getWrappedOp();

        return thisWrapperOp.add(otherWrappedOp);
    }

    @Override
    public Op subtract(Op otherOp) {
        if(!(otherOp instanceof ColumnOp<?>)) {
            throw new RuntimeException("Incompatible Operations");
        }

        ColumnOp otherMapOp = (ColumnOp)otherOp;

        if(getKey() != otherMapOp.getKey()) {
            throw new RuntimeException("Operations are not combinable, since they dont affect the same key");
        }

        Op thisWrapperOp = getWrappedOp();
        Op otherWrappedOp = otherMapOp.getWrappedOp();

        return thisWrapperOp.subtract(otherWrappedOp);
    }

    @Override
    public byte[] serialize() {
        return getWrappedOp().serialize();
    }

    @Override
    public Object deserialize(byte[] serObj) {
        return getWrappedOp().deserialize(serObj);
    }
}
