package com.urbanairship.datacube;

import java.util.Comparator;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class ResultComparator implements Comparator<Result> {
    public static final ResultComparator INSTANCE = new ResultComparator();
    
    @Override
    public int compare(Result arg0, Result arg1) {
        return Bytes.BYTES_RAWCOMPARATOR.compare(arg0.getRow(), arg1.getRow());
    }
};