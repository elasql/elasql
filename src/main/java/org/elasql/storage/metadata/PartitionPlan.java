package org.elasql.storage.metadata;

import java.io.Serializable;

import org.elasql.sql.RecordKey;

public abstract class PartitionPlan implements Serializable {
	
	private static final long serialVersionUID = 1L;

	/**
	 * Check if a record is fully replicated on each node.
	 * 
	 * @param key
	 *            the key of the record
	 * @return if the record is fully replicated
	 */
	public abstract boolean isFullyReplicated(RecordKey key);

	/**
	 * Query the belonging partition.
	 * 
	 * @param key
	 *            the key of the record
	 * @return the id of the partition
	 */
	public abstract int getPartition(RecordKey key);
	
	/**
	 * If this partition plan is a wrapper, returns the underlying
	 * partition plan.
	 * 
	 * @return
	 */
	public abstract PartitionPlan getBasePartitionPlan();
	
	/**
	 * Check if this is the base partition plan, which means that
	 * there is no underlying partition plan inside.
	 * 
	 * @return
	 */
	public abstract boolean isBasePartitionPlan();
	
	public abstract void changeBasePartitionPlan(PartitionPlan plan);
	
	public int numberOfPartitions() {
		return PartitionMetaMgr.NUM_PARTITIONS;
	}

}
