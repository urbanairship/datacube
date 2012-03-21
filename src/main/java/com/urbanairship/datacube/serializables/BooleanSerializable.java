package com.urbanairship.datacube.serializables;

import com.urbanairship.datacube.CSerializable;

/**
*   Use this in your bucketer if you're using booleans as dimension coordinates.
 */
public class BooleanSerializable implements CSerializable{
    private final Boolean bool;

    public BooleanSerializable(Boolean bool){
        this.bool = bool;
    }

    @Override
    public byte[] serialize() {
        byte[] value = new byte[0];
        if (this.bool){
            value = new byte[1];
        }
        return value;
    }
}
