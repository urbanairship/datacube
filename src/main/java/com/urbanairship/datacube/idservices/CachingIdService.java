package com.urbanairship.datacube.idservices;

import org.apache.commons.lang.NotImplementedException;

import com.urbanairship.datacube.Dimension;
import com.urbanairship.datacube.IdService;

/**
 * An IdService that wraps around another IdService and caches its results. Calls to getId() are
 * served from the cache if present, or passed on to the wrapped IdService if not present.
 * 
 * Since input->uniqueID mappings are required to be immutable and consistent between nodes, 
 * caching is straightforward.
 */
public class CachingIdService implements IdService {
    private final IdService wrappedIdService;
    
    public CachingIdService(IdService wrappedIdService) {
        this.wrappedIdService = wrappedIdService;
    }
    
    
    @Override
    public byte[] getId(Dimension<?> dimension, byte[] bytes) {
        throw new NotImplementedException();
    }
}
