package com.urbanairship.datacube;

import java.util.Map;

import com.google.common.collect.Maps;

public class Batch<T extends Op> {
    private final Map<Address,T> map;
    
    Batch() {
        this.map = Maps.newHashMap();
    }
    
    Batch(Map<Address,T> map) {
        this.map = map;
    }
    
    public void putAll(Batch<T> b) {
        this.putAll(b.getMap());
    }
    
    public void putAll(Map<Address,T> other) {
        for(Map.Entry<Address,T> entry: other.entrySet()) {
            Address c = entry.getKey();
            T alreadyExistingVal = map.get(entry.getKey());
            T newVal;
            if(alreadyExistingVal == null) {
                newVal = entry.getValue();
            } else {
                newVal = (T)alreadyExistingVal.add(entry.getValue());
            }
            this.map.put(c, newVal);
        }
    }
    
    public Map<Address,T> getMap() {
        return map;
    }
    
    public String toString() {
        return map.toString();
    }
}
