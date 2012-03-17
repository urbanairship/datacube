package com.urbanairship.datacube.idservices;

import java.io.IOException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.urbanairship.datacube.BoxedByteArray;
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
    private final static Logger log = LogManager.getLogger(CachingIdService.class);
    
    private final LoadingCache<DimensionAndBytes,byte[]> readThroughCache;
    
    public CachingIdService(int numCached, final IdService wrappedIdService) {
        readThroughCache = CacheBuilder.newBuilder()
                .maximumSize(numCached)
                // We expect cache eviction to occur most often due to the size limit. However
                // we also add a time limit so old eunused entries won't sit around on the
                // heap forever.
                .expireAfterAccess(1, TimeUnit.HOURS)
                .removalListener(new RemovalListener<DimensionAndBytes,byte[]>() {
                    @Override
                    public void onRemoval(RemovalNotification<DimensionAndBytes, byte[]> notification) {
                        if(log.isDebugEnabled()) {
                            log.debug("Evicting cache key " + notification.getKey() + ", value " + 
                                    Hex.encodeHexString(notification.getValue()));
                        }
                    }
                })
                .build(new CacheLoader<DimensionAndBytes,byte[]>() {
                    @Override
                    public byte[] load(final DimensionAndBytes key) throws IOException  {
                        byte[] uniqueId = wrappedIdService.getId(key.dimension, key.bytes.bytes);
                        if(log.isDebugEnabled()) {
                            log.debug("Cache loader for key " + key + " returning " +
                                    Hex.encodeHexString(uniqueId));
                        }
                        return uniqueId;
                    }
                });
    }
    
    
    @Override
    public byte[] getId(Dimension<?> dimension, byte[] bytes) throws IOException {
        try {
            return readThroughCache.get(new DimensionAndBytes(dimension,
                    new BoxedByteArray(bytes)));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if(cause instanceof IOException) {
                throw (IOException)cause;
            } else {
                throw new RuntimeException(e);
            }
        }
                
    }
    
    private class DimensionAndBytes {
        private final Dimension<?> dimension;
        private final BoxedByteArray bytes;
        
        public DimensionAndBytes(Dimension<?> dimension, BoxedByteArray bytes) {
            this.dimension = dimension;
            this.bytes = bytes;
        }
        
        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((bytes == null) ? 0 : bytes.hashCode());
            result = prime * result + ((dimension == null) ? 0 : dimension.hashCode());
            return result;
        }
        
        @Override
        public boolean equals(Object obj) {
//            log.error("*** equals running with this=" + this + " and other=" + obj);
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            DimensionAndBytes other = (DimensionAndBytes) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (bytes == null) {
                if (other.bytes != null)
                    return false;
            } else if (!bytes.equals(other.bytes))
                return false;
            if (dimension == null) {
                if (other.dimension != null)
                    return false;
            } else if (!dimension.equals(other.dimension))
                return false;
            return true;
        }
        private CachingIdService getOuterType() {
            return CachingIdService.this;
        }
        
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            sb.append(dimension);
            sb.append(",");
            sb.append(bytes);
            sb.append(")");
            return sb.toString();
        }
        
    }
}
