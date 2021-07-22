package org.elasql.storage.metadata;

import org.elasql.sql.PartitioningKey;
import org.elasql.sql.PrimaryKey;

public abstract class PartitionPlan {
	
	/**
	 * Check if a record is fully replicated on each node.
	 * 
	 * @param key
	 *            the key of the record
	 * @return if the record is fully replicated
	 */
	public abstract boolean isFullyReplicated(PrimaryKey key);

	/**
	 * Query the belonging partition.
	 * 
	 * @param key
	 *            the key of the record
	 * @return the id of the partition
	 */
	public abstract int getPartition(PrimaryKey key);
	
	public abstract PartitionPlan getBasePlan();
	
	public abstract void setBasePlan(PartitionPlan plan);
	
	public int numberOfPartitions() {
		return PartitionMetaMgr.NUM_PARTITIONS;
	}
	
	public abstract PartitioningKey getPartitioningKey(PrimaryKey key);
}
