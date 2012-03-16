package com.urbanairship.datacube;

public class WriteAddressBuilder {
    private final WriteAddress address;
    private boolean built = false;
    
    public WriteAddressBuilder(DataCube<?> cube) {
        this.address = new WriteAddress(cube);
    }
    
    public <F> WriteAddressBuilder at(Dimension<F> dimension, F coordinate) {
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
