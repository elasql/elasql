package org.elasql.storage.metadata;

import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;

public class HashPartitionPlan extends PartitionPlan {
	
	private int numOfParts;
	private String partField;
	
	public HashPartitionPlan() {
		numOfParts = PartitionMetaMgr.NUM_PARTITIONS;
	}
	
	public HashPartitionPlan(String partitionField) {
		numOfParts = PartitionMetaMgr.NUM_PARTITIONS;
		partField = partitionField;
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
		if (partField != null) {
			// XXX: only works for YCSB
			Constant idCon = key.getKeyVal(partField);
			int id = Integer.parseInt((String) idCon.asJavaVal());
			return id % numOfParts;
		} else
			return key.hashCode() % numOfParts;
	}
	
	@Override
	public int numberOfPartitions() {
		return numOfParts;
	}
}
