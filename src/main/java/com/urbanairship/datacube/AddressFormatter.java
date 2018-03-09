package com.urbanairship.datacube;

import java.io.IOException;
import java.util.Optional;

public interface AddressFormatter {
    Optional<byte[]> toKey(Address address, boolean readOnly) throws IOException, InterruptedException;
    Optional<Address> fromKey(byte[] bytes) throws IOException, InterruptedException;
}
