/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import com.google.common.collect.Maps;

import java.util.Map;

public class Batch<T extends Op> {
    private Map<Address, T> map;

    /**
     * Normally you should not create your own Batches but instead have {@link DataCubeIo} create
     * them for you. You can use this if you intend to bypass the high-level magic and you really
     * know what you're doing.
     */
    public Batch() {
        this.map = Maps.newHashMap();
    }

    /**
     * Normally you should not create your own Batches but instead have {@link DataCubeIo} create
     * them for you. You can use this if you intend to bypass the high-level magic and you really
     * know what you're doing.
     *
     * @param map some Ops to wrap in this Batch
     */
    public Batch(Map<Address, T> map) {
        this.map = map;
    }

    public void putAll(Batch<T> b) {
        this.putAll(b.getMap());
    }

    @SuppressWarnings("unchecked")
    public void putAll(Map<Address, T> other) {
        for (Map.Entry<Address, T> entry : other.entrySet()) {
            Address c = entry.getKey();
            T alreadyExistingVal = map.get(entry.getKey());
            T newVal;
            if (alreadyExistingVal == null) {
                newVal = entry.getValue();
            } else {
                newVal = (T) alreadyExistingVal.add(entry.getValue());
            }
            this.map.put(c, newVal);
        }
    }

    public Map<Address, T> getMap() {
        return map;
    }

    public String toString() {
        return map.toString();
    }

    public void reset() {
        map = Maps.newHashMap();
    }
}
