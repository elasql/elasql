package org.elasql.storage.metadata;

import java.io.Serializable;

import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;

public class RangePartitionPlan extends PartitionPlan implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	// We expect that the field is an integer field
	private final String partField;
	private final int recsPerPart;
	private final int numOfParts;
	
	public RangePartitionPlan(String partitionField, int numberOfRecords) {
		partField = partitionField;
		numOfParts = PartitionMetaMgr.NUM_PARTITIONS;
		recsPerPart = numberOfRecords / numOfParts;
	}
	
	public RangePartitionPlan(String partitionField, int numberOfRecords, int numberOfPartitions) {
		partField = partitionField;
		numOfParts = numberOfPartitions;
		recsPerPart = numberOfRecords / numOfParts;
	}
	
	public RangePartitionPlan scaleOut() {
		return new RangePartitionPlan(partField, recsPerPart * numOfParts, numOfParts + 1);
	}
	
	public RangePartitionPlan scaleIn() {
		return new RangePartitionPlan(partField, recsPerPart * numOfParts, numOfParts - 1);
	}

	@Override
	public boolean isFullyReplicated(RecordKey key) {
		return false;
	}

	@Override
	public int getPartition(RecordKey key) {
		// Partitions each item id through mod.
		Constant idCon = key.getKeyVal(partField);
		if (idCon != null) {
			int id = Integer.parseInt((String) idCon.asJavaVal());
			return (id - 1) / recsPerPart;
		}
		
		throw new RuntimeException("Cannot find a partition for " + key);
	}
	
	public String getPartField() {
		return partField;
	}
	
	public int getRecsPerPart() {
		return recsPerPart;
	}
	
	@Override
	public int numberOfPartitions() {
		return numOfParts;
	}
	
	@Override
	public String toString() {
		return String.format("Range Partition: [%d partitions on '%s', each partition has %d records]",
				numOfParts, partField, recsPerPart);
	}
}
