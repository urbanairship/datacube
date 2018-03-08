package com.urbanairship.datacube.dbharnesses;

import com.urbanairship.datacube.Op;

public interface AfterExecute<T extends Op> {
    void afterExecute(Throwable t);
}