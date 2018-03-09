package com.urbanairship.datacube;

import org.apache.hadoop.hbase.util.Bytes;

class BytesOp implements Op<byte[]> {
    public final byte[] bytes;

    public BytesOp(byte[] bytes) {
        this.bytes = bytes;
    }

    @Override
    public byte[] serialize() {
        return bytes;
    }

    @Override
    public Op<byte[]> add(Op otherOp) {
        long otherAsLong = Bytes.toLong(((BytesOp) otherOp).bytes);
        long thisAsLong = Bytes.toLong(this.bytes);
        long added = thisAsLong + otherAsLong;

        return new BytesOp(Bytes.toBytes(added));
    }

    @Override
    public Op<byte[]> subtract(Op otherOp) {
        long otherAsLong = Bytes.toLong(((BytesOp) otherOp).bytes);
        long thisAsLong = Bytes.toLong(this.bytes);
        long subtracted = thisAsLong - otherAsLong;

        return new BytesOp(Bytes.toBytes(subtracted));
    }

    @Override
    public byte[] get() {
        return bytes;
    }
}
