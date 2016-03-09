package com.urbanairship.datacube.dimensionholder;

/**
 * To get dimensions from the row key, we need meta-data about
 * the cube (its dimensions and map of (column qualifier name and operation(LongOp, IntOp etc))that
 * the cube is using for aggregating in each column of the data cube table.
 * so this dimensionHolder class stores these meta-data of the cube.
 */

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.collect.ImmutableList;
import com.urbanairship.datacube.Dimension;
import com.urbanairship.datacube.operations.OpDeserializers;
import de.javakaffee.kryoserializers.KryoReflectionFactorySupport;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Map;

public class DimensionHolder {
    private ArrayList<Dimension<?>> dimensionArrayList;
    private Map<String, OpDeserializers> qualifierOperationMap;
    private int rowKeyPrefixLength;
    private static ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>() {
        protected Kryo initialValue() {
            Kryo kryo = new KryoReflectionFactorySupport() {
                @Override
                public Serializer<?> getDefaultSerializer(final Class clazz) {
                    if (ImmutableList.class.isAssignableFrom(clazz)) {
                        return new ImmutableListSerializer();
                    }
                    return super.getDefaultSerializer(clazz);
                }
            };
            return kryo;
        }
    };

    public DimensionHolder(ArrayList<Dimension<?>> dimensionArrayList,
            Map<String, OpDeserializers> qualifierOperationMap, int rowKeyPrefixLength) {
        this.dimensionArrayList = dimensionArrayList;
        this.qualifierOperationMap = qualifierOperationMap;
        this.rowKeyPrefixLength = rowKeyPrefixLength;
    }

    public byte[] serialize() {
        final Kryo kryo = kryos.get();
        ImmutableListSerializer.registerSerializers(kryo);
        Output output = new Output(new ByteArrayOutputStream());
        kryo.writeObject(output, this);
        byte[] objectBytes = output.toBytes();
        output.close();
        return objectBytes;
    }

    public static DimensionHolder deserialize(byte[] byteArray) {
        final Kryo kryo = kryos.get();
        ImmutableListSerializer.registerSerializers(kryo);
        Input input = new Input(new ByteArrayInputStream(byteArray));
        DimensionHolder readObject;
        readObject = kryo.readObject(input, DimensionHolder.class);
        input.close();
        return readObject;
    }

    public ArrayList<Dimension<?>> getDimensionArrayList() {
        return dimensionArrayList;
    }

    public Map<String, OpDeserializers> getqualifierOperationMap() {
        return qualifierOperationMap;
    }

    public int getRowKeyPrefixLength() { return rowKeyPrefixLength; }
}
