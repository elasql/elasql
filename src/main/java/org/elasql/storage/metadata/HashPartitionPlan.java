package org.elasql.storage.metadata;

import org.elasql.sql.RecordKey;

public class HashPartitionPlan extends PartitionPlan {
	
	private int numOfParts;
	
	public HashPartitionPlan() {
		numOfParts = PartitionMetaMgr.NUM_PARTITIONS;
	}
	
	public HashPartitionPlan(int numberOfPartitions) {
		numOfParts = numberOfPartitions;
	}

	@Override
	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}

	@Override
	public int getPartition(RecordKey key) {
		return key.hashCode() % numOfParts;
	}
	
	@Override
	public int numberOfPartitions() {
		return numOfParts;
	}
}
