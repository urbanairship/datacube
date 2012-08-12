/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube.serializables;

import org.joda.time.DateTime;

import com.urbanairship.datacube.CSerializable;
import com.urbanairship.datacube.Util;

/**
 * Use this in your bucketer if you're using DateTimes as dimension coordinates.
 */
public class DateTimeSerializable implements CSerializable<DateTime> {
    private final long l;

    public DateTimeSerializable(long l) {
        this.l = l;
    }

    @Override
    public byte[] serialize() {
        return Util.longToBytes(l);
    }

    @Override
    public DateTime deserialize(byte[] serObj) {
        return new DateTime(Util.bytesToLong(serObj));
    }
}
