package org.elasql.storage.metadata;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;

import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.Type;

public class PartitioningKey implements Serializable {

	/**
	 * Use the whole PrimaryKey as a partitioning key.
	 * 
	 * @param key
	 */
	public PartitioningKey(PrimaryKey key) {
		// TODO
		throw new RuntimeException("Unimplemented");
	}

	/**
	 * Use the given field to partition for the given primary key. We
	 * assume there is only one partitioning field.
	 * 
	 * @param key
	 *            the key represents a record
	 * @param partitioningField
	 *            the field to partition
	 */
	public PartitioningKey(PrimaryKey key, String partitioningField) {
		// TODO
		throw new RuntimeException("Unimplemented");
	}

	public String getTableName() {
		// TODO
		throw new RuntimeException("Unimplemented");
	}

	public Constant getVal(String fldName) {
		// TODO
		throw new RuntimeException("Unimplemented");
	}

	@Override
	public String toString() {
		// TODO
		throw new RuntimeException("Unimplemented");
	}

	@Override
	public boolean equals(Object obj) {
		// TODO
		throw new RuntimeException("Unimplemented");
	}

	@Override
	public int hashCode() {
		// TODO
		throw new RuntimeException("Unimplemented");
	}

	private void writeObject(ObjectOutputStream out) throws IOException {
		// TODO
		throw new RuntimeException("Unimplemented");
	}

	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		// TODO
		throw new RuntimeException("Unimplemented");
	}
}
