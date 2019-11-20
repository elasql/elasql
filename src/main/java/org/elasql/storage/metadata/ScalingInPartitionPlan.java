package org.elasql.storage.metadata;

import java.util.ArrayDeque;
import java.util.Deque;

import org.elasql.migration.MigrationRange;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;

// Only scaling-out by one node
public class ScalingInPartitionPlan implements PartitionPlan {
	
	private static final long serialVersionUID = 1L;
	
	// We expect that the field is an integer field
	private final String partField;
	private final int oldRecsPerPart;
	private final int numOfParts;
	private final int numOfRecsPerSlice;
	
	public ScalingInPartitionPlan(String partitionField, int numberOfRecords, int oldNumOfParts) {
		partField = partitionField;
		oldRecsPerPart = numberOfRecords / oldNumOfParts;
		numOfParts = oldNumOfParts - 1;
		numOfRecsPerSlice = oldRecsPerPart / numOfParts;
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
			
			if (oldPartId == numOfParts) {
				int pardId = offset / numOfRecsPerSlice;
				
				// It is possible that (partId == numOfParts)
				return Math.min(pardId, numOfParts - 1);
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
		
		for (int partId = 0; partId < numOfParts - 1; partId++) {
			int migrateStartId = numOfParts * oldRecsPerPart + partId * numOfRecsPerSlice + 1;
			int migrateEndId = numOfParts * oldRecsPerPart + (partId + 1) * numOfRecsPerSlice;
			
			ranges.add(new MigrationRange(targetTable, partField, 
						migrateStartId, migrateEndId, numOfParts, partId));
		}
		
		// The last partition may need to accept a little bit more records
		int partId = numOfParts - 1;
		int migrateStartId = numOfParts * oldRecsPerPart + partId * numOfRecsPerSlice + 1;
		int migrateEndId = (numOfParts + 1) * oldRecsPerPart;
		ranges.add(new MigrationRange(targetTable, partField, 
				migrateStartId, migrateEndId, numOfParts, partId));
		
		return ranges;
	}
	
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder("Scaling-In Range Partition: [");
		
		for (int partId = 0; partId < numOfParts - 1; partId++) {
			int oriStartId = partId * oldRecsPerPart;
			int oriEndId = (partId + 1) * oldRecsPerPart;
			int miraStartId = numOfParts * oldRecsPerPart + partId * numOfRecsPerSlice + 1;
			int miraEndId = numOfParts * oldRecsPerPart + (partId + 1) * numOfRecsPerSlice;
			sb.append(String.format("Partition %d: {%d ~ %d, %d ~ %d}, ", partId,
					oriStartId, oriEndId, miraStartId, miraEndId));
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
				+ "ScalingInPartitionPlan that can be changed");
	}
}
