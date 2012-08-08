/*
Copyright 2012 Urban Airship and Contributors
*/

package com.urbanairship.datacube;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

public class Util {
    public static byte[] longToBytes(long l) {
        return ByteBuffer.allocate(8).putLong(l).array();
    }

    /**
     * Write a big-endian integer into the least significant bytes of a byte array.
     */
    public static byte[] intToBytesWithLen(int x, int len) {
        if(len <= 4) {
            return trailingBytes(intToBytes(x), len);
        } else {
            ByteBuffer bb = ByteBuffer.allocate(len);
            bb.position(len-4);
            bb.putInt(x);
            assert bb.remaining() == 0;
            return bb.array();
        }
    }

    public static long bytesToLong(byte[] bytes) {
        if(bytes.length < 8) {
            throw new IllegalArgumentException("Input array was too small: " +
                    Arrays.toString(bytes));
        }
        
        return ByteBuffer.wrap(bytes).getLong();
    }
    
    /**
     * Call this if you have a byte array to convert to long, but your array might need
     * to be left-padded with zeros (if it is less than 8 bytes long). 
     */
    public static long bytesToLongPad(byte[] bytes) {
        final int padZeros = Math.max(8-bytes.length, 0);
        ByteBuffer bb = ByteBuffer.allocate(8);
        for(int i=0; i<padZeros; i++) {
            bb.put((byte)0);
        }
        bb.put(bytes, 0, 8-padZeros);
        bb.flip();
        return bb.getLong();
    }
    
    public static byte[] intToBytes(int x) {
        ByteBuffer bb = ByteBuffer.allocate(4);
        bb.putInt(x);
        return bb.array();
    }

    public static int bytesToInt(byte[] bytes) {
        return ByteBuffer.wrap(bytes).getInt();
    }

    public static byte[] leastSignificantBytes(long l, int numBytes) {
        byte[] all8Bytes = Util.longToBytes(l);
        return trailingBytes(all8Bytes, numBytes);
    }

    public static byte[] trailingBytes(byte[] bs, int numBytes) {
        return Arrays.copyOfRange(bs, bs.length-numBytes, bs.length);
    }


    
    /**
     * Only use this for testing/debugging. Inefficient.
     */
    public static long countRows(Configuration conf, byte[] tableName) throws IOException {
        HTable hTable = null;
        ResultScanner rs = null;
        long count = 0;
        try {
            hTable = new HTable(conf, tableName);
            rs = hTable.getScanner(new Scan());
            for(@SuppressWarnings("unused") Result r: rs) {
                count++;
            }
            return count;
        } finally {
            if(hTable != null) {
                hTable.close();
            }
            if(rs != null) {
                rs.close();
            }
        }
    }
}
