/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.idservices;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.Timer;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.urbanairship.datacube.BoxedByteArray;
import com.urbanairship.datacube.IdService;
import com.urbanairship.datacube.metrics.Metrics;

import java.io.IOException;

/**
 * An IdService that wraps around another IdService and caches its results. Calls to getOrCreateId() are
 * served from the cache if present, or passed on to the wrapped IdService if not present.
 * <p/>
 * Since input->uniqueID mappings are immutable and consistent between nodes, we don't have to
 * deal with invalidation, so caching is straightforward.
 */
public class CachingIdService implements IdService {
    private final Cache<Key, byte[]> cache;
    private final IdService wrappedIdService;

    private final Timer idGetTime;
    private final boolean cacheMisses;
    private static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    private final Meter cachedNullResult;

    public CachingIdService(int numCached, final IdService wrappedIdService, final String cacheName) {
        this(numCached, wrappedIdService, cacheName, false);
    }

    public CachingIdService(int numCached, final IdService wrappedIdService, final String cacheName, boolean cacheMisses) {
        this.wrappedIdService = wrappedIdService;
        this.cache = CacheBuilder.newBuilder()
                .maximumSize(numCached)
                .softValues()
                .recordStats()
                .build();

        Metrics.gauge(CachingIdService.class, cacheName + " ID Cache size", new Gauge<Long>() {
            @Override
            public Long getValue() {
                return cache.size();
            }
        });

        Metrics.gauge(CachingIdService.class, cacheName + "ID Cache effectiveness", new Gauge<Double>() {
            @Override
            public Double getValue() {
                return cache.stats().hitRate();
            }
        });

        this.idGetTime = Metrics.timer(CachingIdService.class, "id_get", cacheName);
        cachedNullResult = Metrics.meter(CachingIdService.class, cacheName, "cache null");
        this.cacheMisses = cacheMisses;
    }


    @Override
    public byte[] getOrCreateId(int dimensionNum, byte[] bytes, int numIdBytes) throws IOException, InterruptedException {
        final Key key = new Key(dimensionNum, new BoxedByteArray(bytes), numIdBytes);
        byte[] currentVal = cache.getIfPresent(key);
        if (currentVal == null) {
            final byte[] id = wrappedIdService.getOrCreateId(dimensionNum, bytes, numIdBytes);
            cache.put(key, id);
            return id;
        } else {
            return currentVal;
        }
    }

    @Override
    public Optional<byte[]> getId(int dimensionNum, byte[] bytes, int numIdBytes) throws IOException, InterruptedException {
        final Timer.Context time = idGetTime.time();
        try {
            final Key key = new Key(dimensionNum, new BoxedByteArray(bytes), numIdBytes);
            final byte[] cachedVal = cache.getIfPresent(key);

            if (cachedVal == null) {
                final Optional<byte[]> id = wrappedIdService.getId(dimensionNum, bytes, numIdBytes);
                if (id.isPresent()) {
                    cache.put(key, id.get());
                } else if (cacheMisses) {
                    cachedNullResult.mark();
                    cache.put(key, EMPTY_BYTE_ARRAY);
                }
                return id;
            } else if (cachedVal == EMPTY_BYTE_ARRAY) {
                return Optional.absent();
            } else {
                return Optional.of(cachedVal);
            }
        } finally {
            time.stop();
        }
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
