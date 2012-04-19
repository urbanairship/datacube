package com.urbanairship.datacube.backfill;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.io.Writable;

/**
 * A Hadoop Writable that serializes an arbitrary Collection of Writables.
 * 
 * The format is elementClassName + numElements + elementsSerialized
 */
public class CollectionWritable implements Writable {
    private Collection<? extends Writable> collection;
    private Class<? extends Writable> elementClass;
    
    public CollectionWritable() { } // No-op constructor is required for deserializing
    
    public CollectionWritable(Class<? extends Writable> elementClass, 
            Collection<? extends Writable> collection) {
        this.elementClass = elementClass;
        this.collection = collection;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readFields(DataInput in) throws IOException {
        try {
            String valueClassName = in.readUTF();
            elementClass = (Class<? extends Writable>)Class.forName(valueClassName);
            if(!Writable.class.isAssignableFrom(elementClass)) {
                throw new RuntimeException("valueClass is not writable: " + valueClassName);
            }
            int numElements = in.readInt();
            List<Writable> newList = new ArrayList<Writable>(numElements);
            for(int i=0; i<numElements; i++) {
                Writable instance = elementClass.newInstance();
                instance.readFields(in);
                newList.add(instance);
            }
            collection = newList;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(elementClass.getName());
        out.writeInt(collection.size());
        for(Writable writable: collection) {
            writable.write(out);
        }
    }
    
    public Collection<? extends Writable> getCollection() {
        return collection;
    }
}


