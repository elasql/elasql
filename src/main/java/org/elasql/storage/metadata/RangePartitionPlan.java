package org.elasql.storage.metadata;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;

import org.elasql.migration.MigrationRange;
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
	
	public ScalingOutPartitionPlan scaleOut() {
//		return new RangePartitionPlan(partField, recsPerPart * numOfParts, numOfParts + 1);
		return new ScalingOutPartitionPlan(partField, recsPerPart * numOfParts, numOfParts);
	}
	
	public ScalingInPartitionPlan scaleIn() {
//		return new RangePartitionPlan(partField, recsPerPart * numOfParts, numOfParts - 1);
		return new ScalingInPartitionPlan(partField, recsPerPart * numOfParts, numOfParts);
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
			// XXX: This parsing only works for YCSB
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
	
	public Deque<MigrationRange> generateMigrationRanges(
			RangePartitionPlan oldPartPlan, String targetTable) {
		Deque<MigrationRange> ranges = new ArrayDeque<MigrationRange>();
		
		for (int oldPart = 0; oldPart < oldPartPlan.numOfParts; oldPart++) {
			int oldStartId = oldPart * oldPartPlan.recsPerPart + 1;
			int oldEndId = (oldPart + 1) * oldPartPlan.recsPerPart;
			
			for (int newPart = 0; newPart < numOfParts; newPart++) {
				// We do not need to migrate the data on the same partition
				if (oldPart == newPart)
					continue;
				
				int newStartId = newPart * recsPerPart + 1;
				int newEndId = (newPart + 1) * recsPerPart;
				
				// If there is no overlap, there would be nothing to be migrated.
				if (oldStartId > newEndId || oldEndId < newStartId)
					continue;
				
				// Take the overlapping range
				int migrateStartId = Math.max(oldStartId, newStartId);
				int migrateEndId = Math.min(oldEndId, newEndId);
				
				ranges.add(new MigrationRange(targetTable, partField, 
						migrateStartId, migrateEndId, oldPart, newPart));
			}
		}
		
		return ranges;
	}
	
	@Override
	public String toString() {
		return String.format("Range Partition: [%d partitions on '%s', each partition has %d records]",
				numOfParts, partField, recsPerPart);
	}
}
