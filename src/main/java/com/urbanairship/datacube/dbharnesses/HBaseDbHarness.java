package com.urbanairship.datacube.dbharnesses;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang.NotImplementedException;

import com.google.common.base.Optional;
import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.Batch;
import com.urbanairship.datacube.DbHarness;
import com.urbanairship.datacube.Op;

public class HBaseDbHarness<T extends Op> implements DbHarness<T> {
    
    @Override
    public Optional<T> get(Address c) throws IOException {
        throw new NotImplementedException();
    }

    @Override
    public void runBatch(Batch<T> batch) throws IOException {
        throw new NotImplementedException();
    }
    
    @Override
    public List<Optional<T>> multiGet(List<Address> addresses) throws IOException {
        throw new NotImplementedException();
    }
}
