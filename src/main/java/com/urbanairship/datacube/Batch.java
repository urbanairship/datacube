package com.urbanairship.datacube;

import java.util.HashMap;
import java.util.Map;

public class Batch<T extends Op> {
    private final Map<Coords,T> map;
    
    Batch() {
        this.map = new HashMap<Coords,T>();
    }
    
    Batch(Map<Coords,T> map) {
        this.map = map;
    }
    
    public void putAll(Batch<T> b) {
        this.putAll(b.getMap());
    }
    
    public void putAll(Map<Coords,T> other) {
        for(Map.Entry<Coords,T> entry: other.entrySet()) {
            Coords c = entry.getKey();
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
    
    public Map<Coords,T> getMap() {
        return map;
    }
}
