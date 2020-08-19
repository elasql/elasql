package org.elasql.sql;

import org.vanilladb.core.sql.Constant;

public class PartitioningKey extends Key {

	private static final long serialVersionUID = 20200819001L;

	/**
	 * Use the whole PrimaryKey as a partitioning key.
	 * 
	 * @param key
	 */
	public static PartitioningKey fromPrimaryKey(PrimaryKey key) {
		return new PartitioningKey(key);
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
	public static PartitioningKey fromPrimaryKey(PrimaryKey key, String partitioningField) {
		Constant value = key.getVal(partitioningField);
		if (value == null)
			throw new IllegalArgumentException(String.format(
					"There is no field %s in key %s", partitioningField, key));
		
		return new PartitioningKey(key.getTableName(), partitioningField, value);
	}

	public PartitioningKey(String tableName, String fld, Constant val) {
		super(tableName, fld, val);
	}
	
	private PartitioningKey(PrimaryKey key) {
		super(key);
	}
}
