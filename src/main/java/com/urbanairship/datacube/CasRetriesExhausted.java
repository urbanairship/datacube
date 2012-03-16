package com.urbanairship.datacube;

import java.io.IOException;

/**
 * This will be thrown if we try to compare-and-swap to update a value, but fail too many
 * times due to concurrent modification by other threads/processes.
 */
public class CasRetriesExhausted extends IOException {
    private static final long serialVersionUID = -5165354852197742419L;

}
