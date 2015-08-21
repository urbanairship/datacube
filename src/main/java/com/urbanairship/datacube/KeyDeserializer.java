package com.urbanairship.datacube;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;
import com.urbanairship.datacube.ops.LongOp;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;

public class KeyDeserializer {
    private final int prefixLen;
    private final ImmutableList<Dimension<?>> dimensions;
    private final Ordering<Dimension<?>> ordering;

    public KeyDeserializer(int prefixLen, ImmutableList<Dimension<?>> dimensions) {
        this.prefixLen = prefixLen;
        this.dimensions = dimensions;
        this.ordering = Ordering.explicit(dimensions);
    }

    public SortedMap<Dimension<?>, KeyPart> parseKey(byte[] rawKey) {
        ByteBuffer key = ByteBuffer.wrap(rawKey);
        skip(key, prefixLen);

        ImmutableSortedMap.Builder<Dimension<?>, KeyPart> result = ImmutableSortedMap.orderedBy(ordering);
        for (Dimension<?> dim : dimensions) {
            if (key.remaining() == 0) {


                continue;
            }

            switch (key.get()) {
                case 0:
                    //Absent, skip empty space and continue
                    skip(key, dim.getBucketPrefixSize() + dim.getNumFieldBytes());
                    break;

                case 1:
                    final byte[] value;
                    final byte[] bucket;

                    if (dim.isBucketed()) {
                        bucket = new byte[dim.getBucketPrefixSize()];
                        key.get(bucket);
                    } else {
                        bucket = new byte[]{};
                    }

                    value = new byte[dim.getNumFieldBytes()];
                    key.get(value);

                    result.put(dim, new KeyPart(value, bucket, dim.getDoIdSubstitution()));
                    break;

                default:
                    throw new IllegalArgumentException("welp");
            }
        }

        if (key.remaining() != 0) {
            throw new IllegalArgumentException("womp");
        }

        return result.build();
    }

    private void skip(ByteBuffer b, int i) {
        for (int j = 0; j < i; j++) {
            b.get();
        }
    }

    public static class KeyPart {
        private static final byte[] PRESENT = new byte[]{1};

        public final byte[] value;
        public final byte[] bucket;
        public final boolean mapped;

        public KeyPart(byte[] value, byte[] bucket, boolean mapped) {
            this.value = value;
            this.bucket = bucket;
            this.mapped = mapped;
        }

        public byte[] toBytes() {
            return Bytes.concat(PRESENT, bucket, value);
        }
    }

    public static class TestCube extends DataCube<LongOp> {



        private static final ImmutableList<Dimension<?>> dimensions = ImmutableList.of();

        private static final List<Rollup> rollups = makeRollups();

        public TestCube() {
            super(dimensions, rollups);
        }



        private static List<Rollup> makeRollups() {
            ImmutableList.Builder<Rollup> builder = ImmutableList.builder();
            Set<Set<Dimension<?>>> sets = Sets.powerSet(ImmutableSortedSet.copyOf(dimensions));

            for (Set<Dimension<?>> set : sets) {
                if (!set.isEmpty()) {
                    builder.add(new Rollup(set.toArray(new Dimension<?>[set.size()])));
                }
            }

            return builder.build();
        }
    }
}
