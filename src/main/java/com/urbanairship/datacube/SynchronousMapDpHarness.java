package com.urbanairship.datacube;

import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

public class SynchronousMapDpHarness<T extends Op, Q> implements SynchronousDbHarness<T, Q> {
    private final ConcurrentHashMap<Address, T> map;
    private Function<T, Q> opValueGetter;

    public SynchronousMapDpHarness(Function<T, Q> opValueGetter) {
        this.opValueGetter = opValueGetter;
        this.map = new ConcurrentHashMap<>();
    }


    @Override
    public Map<Address, Q> get(Set<Address> addressList) {
        ImmutableMap.Builder<Address, Q> builder = ImmutableMap.<Address, Q>builder();
        for (Address address : addressList) {
            T t = map.get(address);

            if (t != null) {
                builder.put(address, opValueGetter.apply(t));
            }
        }
        return builder.build();
    }

    @Override
    public void increment(Map<Address, T> batch) {
        for (Map.Entry<Address, T> entry : batch.entrySet()) {
            map.compute(entry.getKey(), (address, op) -> {
                if (op != null) {
                    return (T) op.add(entry.getValue());
                }
                return entry.getValue();
            });
        }
    }

    @Override
    public void overwrite(Map<Address, T> batch) {
        map.putAll(batch);
    }

    @Override
    public void readCombine(Map<Address, T> batch) {
        // this is the same as increment in this case, since there's no native map.increment method.
        increment(batch);

    }
}
