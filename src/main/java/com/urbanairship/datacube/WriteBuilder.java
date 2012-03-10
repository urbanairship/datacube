package com.urbanairship.datacube;

public class WriteBuilder {
    private final WriteAddress address = new WriteAddress();
    private boolean built = false;
    
    public WriteBuilder at(Dimension dimension, byte[] coordinate) {
        address.at(dimension, coordinate);
        return this;
    }
    
    public WriteAddress build() {
        if(built) {
            throw new RuntimeException("Already built");
        }
        built = true;
        return address;
    }
    
}
