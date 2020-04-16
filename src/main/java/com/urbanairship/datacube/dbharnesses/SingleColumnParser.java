package com.urbanairship.datacube.dbharnesses;

import com.urbanairship.datacube.Deserializer;
import com.urbanairship.datacube.Op;
import org.apache.hadoop.hbase.client.Result;

import java.util.function.Function;

public class SingleColumnParser<T extends Op> implements Function<Deserializer<T>, Function<Result, T>> {

    @Override
    public Function<Result, T> apply(Deserializer<T> deserializer) {
        return result -> deserializer.fromBytes(result.value());
    }
}
