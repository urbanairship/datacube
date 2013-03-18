package com.urbanairship.datacube;

import com.urbanairship.datacube.ops.DoubleOp;
import org.junit.Assert;
import org.junit.Test;

public class OpTests {

    @Test
    public void testDoubleOp() {
        DoubleOp onePt2 = new DoubleOp(1.2);
        byte[] bytes = onePt2.serialize();
        DoubleOp.DoubleOpDeserializer deserializer = new DoubleOp.DoubleOpDeserializer();
        DoubleOp deserialized = deserializer.fromBytes(bytes);
        Assert.assertEquals(onePt2, deserialized);

        Op add = onePt2.add(deserialized);
        Assert.assertEquals(new DoubleOp(2.4), add);
    }

}
