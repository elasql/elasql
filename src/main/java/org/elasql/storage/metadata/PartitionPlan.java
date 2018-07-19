package org.elasql.storage.metadata;

import org.elasql.sql.RecordKey;

public interface PartitionPlan {
	
	/**
	 * Check if a record is fully replicated on each node.
	 * 
	 * @param key
	 *            the key of the record
	 * @return if the record is fully replicated
	 */
	public boolean isFullyReplicated(RecordKey key);

	/**
	 * Query the belonging partition.
	 * 
	 * @param key
	 *            the key of the record
	 * @return the id of the partition
	 */
	public int getPartition(RecordKey key);

}
