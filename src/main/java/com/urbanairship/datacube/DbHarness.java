package com.urbanairship.datacube;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Optional;

public interface DbHarness<T extends Op> {
    public void runBatch(Batch<T> batch) throws IOException;
    
    /**
     * @return absent if the value doesn't exist, or the value if it does.
     */
    public Optional<T> get(Coords c) throws IOException;
    
    public List<Optional<T>> multiGet(List<Coords> coordsList) throws IOException;
}
