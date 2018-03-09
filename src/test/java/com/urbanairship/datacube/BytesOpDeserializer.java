package com.urbanairship.datacube;

class BytesOpDeserializer implements Deserializer<BytesOp> {
    @Override
    public BytesOp fromBytes(byte[] bytes) {
        return new BytesOp(bytes);
    }
}
