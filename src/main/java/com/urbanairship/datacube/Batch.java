package com.urbanairship.datacube;

import java.util.Map;

import com.google.common.collect.Maps;

public class Batch<T extends Op> {
    private final Map<ExplodedAddress,T> map;
    
    Batch() {
        this.map = Maps.newHashMap();
    }
    
    Batch(Map<ExplodedAddress,T> map) {
        this.map = map;
    }
    
    public void putAll(Batch<T> b) {
        this.putAll(b.getMap());
    }
    
    public void putAll(Map<ExplodedAddress,T> other) {
        for(Map.Entry<ExplodedAddress,T> entry: other.entrySet()) {
            ExplodedAddress c = entry.getKey();
            T alreadyExistingVal = map.get(entry.getKey());
            T newVal;
            if(alreadyExistingVal == null) {
                newVal = entry.getValue();
            } else {
                // Contortions to avoid compiler warnings
                newVal = (T)alreadyExistingVal.combine(entry.getValue());
            }
            this.map.put(c, newVal);
        }
    }
    
    public Map<ExplodedAddress,T> getMap() {
        return map;
    }
    
    public String toString() {
        return map.toString();
    }
}
