package org.elasql.storage.metadata;

import java.io.Serializable;
import java.util.ArrayDeque;
import java.util.Deque;

import org.elasql.migration.MigrationRange;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;

// Only scaling-out by one node
public class ScalingOutPartitionPlan extends PartitionPlan implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	// We expect that the field is an integer field
	private final String partField;
	private final int oldRecsPerPart;
	private final int numOfParts;
	private final int cutPointPerPart;
	
	public ScalingOutPartitionPlan(String partitionField, int numberOfRecords, int oldNumOfParts) {
		partField = partitionField;
		oldRecsPerPart = numberOfRecords / oldNumOfParts;
		numOfParts = oldNumOfParts + 1;
		cutPointPerPart = oldRecsPerPart / numOfParts;
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
			
			int oldPartId = (id - 1) / oldRecsPerPart;
			int offset =  (id - 1) % oldRecsPerPart;
			
			if (offset < cutPointPerPart) {
				return numOfParts - 1;
			} else {
				return oldPartId;
			}
		}
		
		throw new RuntimeException("Cannot find a partition for " + key);
	}
	
	@Override
	public int numberOfPartitions() {
		return numOfParts;
	}
	
	public Deque<MigrationRange> generateMigrationRanges(
			RangePartitionPlan oldPartPlan, String targetTable) {
		Deque<MigrationRange> ranges = new ArrayDeque<MigrationRange>();
		
		for (int oldPart = 0; oldPart < oldPartPlan.numberOfPartitions(); oldPart++) {
			int oldStartId = oldPart * oldPartPlan.getRecsPerPart() + 1;
			
			int migrateStartId = oldStartId;
			int migrateEndId = oldStartId + cutPointPerPart - 1;
			
			ranges.add(new MigrationRange(targetTable, partField, 
						migrateStartId, migrateEndId, oldPart, numOfParts - 1));
		}
		
		return ranges;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("Scaling-Out Range Partition: [");
		
		for (int partId = 0; partId < numOfParts - 1; partId++) {
			int startId = partId * oldRecsPerPart + cutPointPerPart + 1;
			int endId = (partId + 1) * oldRecsPerPart;
			sb.append(String.format("Partition %d: %d ~ %d, ", partId, startId, endId));
		}
		
		sb.append(String.format("Partition %d: all other records]", numOfParts - 1));
		
		return sb.toString();
	}

	@Override
	public PartitionPlan getBasePartitionPlan() {
		return this;
	}

	@Override
	public boolean isBasePartitionPlan() {
		return true;
	}

	@Override
	public void changeBasePartitionPlan(PartitionPlan plan) {
		throw new RuntimeException("There is no base partition plan in "
				+ "ScalingOutPartitionPlan that can be changed");
	}
}
