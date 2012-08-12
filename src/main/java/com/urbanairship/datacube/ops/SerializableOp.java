package com.urbanairship.datacube.ops;

import com.urbanairship.datacube.Op;

/**
 * An operation which is able to serialize and deserialize itself
 * @author dmi
 *
 */
public interface SerializableOp extends Op {
	public byte[] serialize();
	public SerializableOp deserialize(byte[] Op);
}
