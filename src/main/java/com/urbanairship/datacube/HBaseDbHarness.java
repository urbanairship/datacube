package com.urbanairship.datacube;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import com.google.common.base.Optional;

public class HBaseDbHarness<T extends Op> implements DbHarness<T> {
    
    @Override
    public Optional<T> get(ExplodedAddress c) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void runBatch(Batch<T> batch) throws IOException {
        throw new NotImplementedException();
    }
    
    @Override
    public List<Optional<T>> multiGet(List<ExplodedAddress> addresses) throws IOException {
        throw new NotImplementedException();
    }
}
