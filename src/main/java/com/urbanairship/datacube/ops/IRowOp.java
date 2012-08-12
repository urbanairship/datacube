package com.urbanairship.datacube.ops;

import java.util.Map;

import com.urbanairship.datacube.Address;
import com.urbanairship.datacube.BoxedByteArray;
import com.urbanairship.datacube.Op;

/**
 * The "root" structure describing an operation performed on a whole row of
 * the key-value store
 * @author dmi
 *
 */
public interface IRowOp extends Op {
	public Address getKey();
	public Map<BoxedByteArray, SerializableOp> getColumnOps();
	public void addColumnOp(BoxedByteArray columnQualifier, SerializableOp columnOp);
	public void addColumnOps(Map<BoxedByteArray, SerializableOp> columnOps);
}
