package com.urbanairship.datacube;

import java.io.IOException;
import java.util.List;

import com.google.common.base.Optional;

/**
 * Interface for drivers connecting a cube to a backing database.
 * 
 * Implementations are not expected to be thread-safe.
 */
public interface DbHarness<T extends Op> {
    public void runBatch(Batch<T> batch) throws IOException;
    
    /**
     * @return absent if the coord doesn't exist, or the coord if it does.
     */
    public Optional<T> get(ExplodedAddress c) throws IOException;
    
    public List<Optional<T>> multiGet(List<ExplodedAddress> addresses) throws IOException;
}
