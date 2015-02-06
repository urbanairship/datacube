package com.urbanairship.datacube.operations;

import com.urbanairship.datacube.ops.DoubleOp;
import com.urbanairship.datacube.ops.IntOp;
import com.urbanairship.datacube.ops.LongOp;

public enum OpDeserializers implements OpDeserializer {
    INT {
        @Override
        public String getValue(byte[] values) {
            IntOp.IntOpDeserializer deserializer = new IntOp.IntOpDeserializer();
            return String.valueOf(deserializer.fromBytes(values).getInt());
        }
    },
    DOUBLE {
        @Override
        public String getValue(byte[] values) {
            DoubleOp.DoubleOpDeserializer deserializer = new DoubleOp.DoubleOpDeserializer();
            return String.valueOf(deserializer.fromBytes(values).getDouble());
        }
    },
    LONG {
        @Override
        public String getValue(byte[] values) {
            LongOp.LongOpDeserializer deserializer = new LongOp.LongOpDeserializer();
            return String.valueOf(deserializer.fromBytes(values).getLong());
        }
    };
}
