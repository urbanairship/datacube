package com.urbanairship.datacube;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

public class Reschemanator {
    private static final ImmutableMap<Integer, byte[]> ABSENT_VALUES = makeAbsentValues();
    private static ImmutableMap<Integer, byte[]> makeAbsentValues() {
        ImmutableMap.Builder<Integer, byte[]> builder = ImmutableMap.builder();
        for (int i = 0; i < 256; i++) {
            builder.put(i, new byte[i]);
        }
        return builder.build();
    }

    private final byte[] prefixBytes;
    private final List<Dimension<?>> from;
    private final List<Dimension<?>> reversedTo;
    private final KeyDeserializer keyDeserializer;

    public static Reschemanator create(String prefix, ImmutableList<Dimension<?>> from, ImmutableList<Dimension<?>> to) {
        Preconditions.checkArgument(StringUtils.isNotBlank(prefix));
        Preconditions.checkNotNull(from);
        Preconditions.checkNotNull(to);
        Preconditions.checkArgument(from.size() == to.size());
        Preconditions.checkArgument(ImmutableSet.copyOf(from).containsAll(to));
        Preconditions.checkArgument(ImmutableSet.copyOf(to).containsAll(from));

        final KeyDeserializer keyDeserializer = new KeyDeserializer(prefix.length(), from);
        return new Reschemanator(prefix.getBytes(), from, Lists.reverse(to), keyDeserializer);
    }

    private Reschemanator(byte[] prefixBytes, List<Dimension<?>> from, List<Dimension<?>> reversedTo, KeyDeserializer keyDeserializer) {
        this.prefixBytes = prefixBytes;
        this.from = from;
        this.reversedTo = reversedTo;
        this.keyDeserializer = keyDeserializer;
    }

    public byte[] newKey(byte[] oldKey) {
        final SortedMap<Dimension<?>, KeyDeserializer.KeyPart> inputKeyParts = keyDeserializer.parseKey(oldKey);
        final ArrayList<byte[]> outputKeyParts = Lists.newArrayListWithCapacity(from.size() + 1);

        boolean setKeyFound = false;
        for (Dimension<?> dimension : reversedTo) {
            final KeyDeserializer.KeyPart keyPart = inputKeyParts.get(dimension);

            if (keyPart != null) {
                setKeyFound = true;
                outputKeyParts.add(keyPart.toBytes());

            } else if (setKeyFound) {//If not, leave off the end of the key
                //Absent key, need a 0 byte, followed by a number of bytes depending on dimension
                final byte[] absentBytes = Preconditions.checkNotNull(
                        ABSENT_VALUES.get(1 + dimension.getBucketPrefixSize() + dimension.getNumFieldBytes()));
                outputKeyParts.add(absentBytes);

            }
        }
        outputKeyParts.add(prefixBytes);

        return concat(outputKeyParts);
    }

    // Copied with minor modifications from Guava's Bytes class
    private byte[] concat(List<byte[]> reversedParts) {
        int length = 0;
        for (byte[] array : reversedParts) {
            length += array.length;
        }
        byte[] result = new byte[length];
        int pos = 0;
        for (byte[] array : Lists.reverse(reversedParts)) {
            System.arraycopy(array, 0, result, pos, array.length);
            pos += array.length;
        }
        return result;
    }
}
