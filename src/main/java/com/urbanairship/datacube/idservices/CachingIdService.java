/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.idservices;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Gauge;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.urbanairship.datacube.BoxedByteArray;
import com.urbanairship.datacube.IdService;

/**
 * An IdService that wraps around another IdService and caches its results. Calls to getId() are
 * served from the cache if present, or passed on to the wrapped IdService if not present.
 * 
 * Since input->uniqueID mappings are immutable and consistent between nodes, we don't have to
 * deal with invalidation, so caching is straightforward.
 */
public class CachingIdService implements IdService {
    private final static Logger log = LoggerFactory.getLogger(CachingIdService.class);
    
    private final LoadingCache<Key,byte[]> readThroughCache;

    private final Gauge cacheSize;
    private final Gauge cacheEffectiveness;
    
    public CachingIdService(int numCached, final IdService wrappedIdService, final String cacheName) {
        readThroughCache = CacheBuilder.newBuilder()
                .maximumSize(numCached)
                .softValues()
                .recordStats()
                .removalListener(new RemovalListener<Key, byte[]>() {
                    @Override
                    public void onRemoval(RemovalNotification<Key, byte[]> notification) {
                        if (log.isDebugEnabled()) {
                            log.debug("Evicting cache key " + notification.getKey() + ", value " +
                                    Hex.encodeHexString(notification.getValue()));
                        }
                    }
                })
                .build(new CacheLoader<Key,byte[]>() {
                    @Override
                    public byte[] load(final Key key) throws IOException, InterruptedException  {
                        byte[] uniqueId = wrappedIdService.getId(key.dimensionNum, 
                                key.bytes.bytes, key.idLength);
                        if(log.isDebugEnabled()) {
                            log.debug("Cache loader for key " + key + " returning " +
                                    Hex.encodeHexString(uniqueId));
                        }
                        return uniqueId;
                    }
                });

        cacheSize = Metrics.newGauge(CachingIdService.class, cacheName+" ID Cache size", new Gauge<Long>() {
            @Override
            public Long value() {
                return readThroughCache.size();
            }
        });

        cacheEffectiveness = Metrics.newGauge(CachingIdService.class, cacheName+"ID Cache effectiveness", new Gauge<Double>() {
            @Override
            public Double value() {
                return readThroughCache.stats().hitRate();
            }
        });
    }
    
    
    @Override
    public byte[] getId(int dimensionNum, byte[] bytes, int numIdBytes) throws IOException, InterruptedException {
        try {
            return readThroughCache.get(new Key(dimensionNum, new BoxedByteArray(bytes), 
                    numIdBytes));
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if(cause instanceof IOException) {
                throw (IOException)cause;
            } else if (cause instanceof InterruptedException) {
                throw (InterruptedException)cause;
            } else {
                throw new RuntimeException(e);
            }
        }
                
    }

    @Override
    public byte[] serialize() {
        try {
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream);
            outputStream.writeInt(CachingIdService.class.getName().length());
            outputStream.write(CachingIdService.class.getName().getBytes());
            return byteArrayOutputStream.toByteArray();
        } catch (Exception e) {
            log.error("Exception in idService serialization of CachingIdService "+e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public byte[] getCoordinate(int dimensionIndex, byte[] byteArray) {
        log.error("Can not get coordinate from CachingIdService");
        throw new RuntimeException("Can not get coordinate from CachingIdService");
    }

    private class Key {
        private final int dimensionNum;
        private final BoxedByteArray bytes;
        private final int idLength;
        
        public Key(int dimensionNum, BoxedByteArray bytes, int idLength) {
            this.dimensionNum = dimensionNum;
            this.bytes = bytes;
            this.idLength = idLength;
        }

        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("(");
            sb.append(dimensionNum);
            sb.append(",");
            sb.append(bytes);
            sb.append(")");
            return sb.toString();
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((bytes == null) ? 0 : bytes.hashCode());
            result = prime * result + dimensionNum;
            result = prime * result + idLength;
            return result;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            Key other = (Key) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (bytes == null) {
                if (other.bytes != null)
                    return false;
            } else if (!bytes.equals(other.bytes))
                return false;
            if (dimensionNum != other.dimensionNum)
                return false;
            if (idLength != other.idLength)
                return false;
            return true;
        }

        private CachingIdService getOuterType() {
            return CachingIdService.this;
        }
    }
}
