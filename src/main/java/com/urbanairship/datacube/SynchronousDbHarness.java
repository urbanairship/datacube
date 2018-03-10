package com.urbanairship.datacube;

import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

/**
 *
 * @param <T> The type of operation being applied.
 * @param <Q> The value of the operation.
 */
public interface SynchronousDbHarness<T extends Op, Q> {
    /**
     * Retrieve the values for all of the specified addresses
     *
     * @param addressList
     *
     * @return A map from whatever supplied addresses were found in the database to the value retrieved from the database
     */
    Map<Address, Q> get(Set<Address> addressList);

    /**
     * See {@link #get}
     *
     * @param addressList
     *
     * @return
     */
    default Map<Address, Q> get(Address... addressList) {
        return get(ImmutableSet.<Address>builder().add(addressList).build());
    }


    /**
     * apply the op as an increment operation, if your database supports it.
     *
     * @param batch
     */
    void increment(Map<Address, T> batch);

    /**
     * apply the results of the batch regardless of what was there previously.
     *
     * @param batch
     */
    void overwrite(Map<Address, T> batch);

    /**
     * Read whatever is in the database, apply the op to it, write the results to the database.
     *
     * @param batch
     */
    void readCombine(Map<Address, T> batch);


}
