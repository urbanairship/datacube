/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

public interface CSerializable<T> {
    public byte[] serialize();
    public T deserialize(byte[] serObj);
}
