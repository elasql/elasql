package org.elasql.storage.metadata;

import org.elasql.sql.PartitioningKey;
import org.elasql.sql.PrimaryKey;

public class HashPartitionPlan extends PartitionPlan {
	
	private int numOfParts;
	
	public HashPartitionPlan() {
		numOfParts = PartitionMetaMgr.NUM_PARTITIONS;
	}
	
	public HashPartitionPlan(int numberOfPartitions) {
		numOfParts = numberOfPartitions;
	}

	@Override
	public boolean isFullyReplicated(PrimaryKey key) {
		return false;
	}

	@Override
	public int getPartition(PrimaryKey key) {
		return key.hashCode() % numOfParts;
	}
	
	@Override
	public int numberOfPartitions() {
		return numOfParts;
	}

	@Override
	public PartitionPlan getBasePlan() {
		return this;
	}

	@Override
	public void setBasePlan(PartitionPlan plan) {
		new UnsupportedOperationException();
	}
	
	@Override
	public PartitioningKey getPartitioningKey(PrimaryKey key) {
		return PartitioningKey.fromPrimaryKey(key);
	}
}
