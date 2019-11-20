package org.elasql.storage.metadata;

import java.io.Serializable;

import org.elasql.sql.RecordKey;

public interface PartitionPlan extends Serializable {

	/**
	 * Check if a record is fully replicated on each node.
	 * 
	 * @param key
	 *            the key of the record
	 * @return if the record is fully replicated
	 */
	boolean isFullyReplicated(RecordKey key);

	/**
	 * Query the belonging partition.
	 * 
	 * @param key
	 *            the key of the record
	 * @return the id of the partition
	 */
	int getPartition(RecordKey key);
	
	/**
	 * If this partition plan is a wrapper, returns the underlying
	 * partition plan.
	 * 
	 * @return
	 */
	PartitionPlan getBasePartitionPlan();
	
	/**
	 * Check if this is the base partition plan, which means that
	 * there is no underlying partition plan inside.
	 * 
	 * @return
	 */
	boolean isBasePartitionPlan();
	
	void changeBasePartitionPlan(PartitionPlan plan);
	
	int numberOfPartitions();

}
