package org.elasql.schedule.tpart;

import java.util.Arrays;
import java.util.List;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class HotAvoidanceRouter implements BatchNodeInserter {
	
	private static final int PART_TO_AVOID = 0;
	
	private PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();

	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			insert(graph, task);
		}
	}
	
	private void insert(TGraph graph, TPartStoredProcedureTask task) {
		int destination = findBestDestination(graph, task);
		
		// Debug
		if (task.getTxNum() % 5000 == 0) {
			System.out.println("Final: " + destination);
		}
		
		graph.insertTxNode(task, destination);
	}
	
	private int findBestDestination(TGraph graph, TPartStoredProcedureTask task) {
		int[] readDistribution = new int[PartitionMetaMgr.NUM_PARTITIONS];
		
		// Count reads from each partition
		for (PrimaryKey key : task.getReadSet()) {
			// Skip replicated records
			if (partMgr.isFullyReplicated(key))
				continue;
			
			int partId = graph.getResourcePosition(key).getPartId();
			readDistribution[partId]++;
		}
		
		// Find the partition that has the most reads
		int mostReadPartId = 0;
		for (int partId = 1; partId < PartitionMetaMgr.NUM_PARTITIONS; partId++) {
			if (readDistribution[partId] > readDistribution[mostReadPartId]) {
				mostReadPartId = partId;
			}
		}
		
		// Debug
		if (task.getTxNum() % 5000 == 0) {
			System.out.println(String.format("========= Tx.%d ==========", task.getTxNum()));
			System.out.println("Reads: " + Arrays.toString(readDistribution));
			System.out.println("Most Read: " + mostReadPartId);
		}
		
		// If that is the partition to avoid, try to find the second most one (if exists)
		if (mostReadPartId != PART_TO_AVOID)
			return mostReadPartId;
		
		int secondMostPartId = (mostReadPartId + 1) % PartitionMetaMgr.NUM_PARTITIONS;
		for (int offset = 2; offset < PartitionMetaMgr.NUM_PARTITIONS; offset++) {
			int partId = (mostReadPartId + offset) % PartitionMetaMgr.NUM_PARTITIONS;
			if (readDistribution[partId] > readDistribution[secondMostPartId]) {
				secondMostPartId = partId;
			}
		}
		
		// Debug
		if (task.getTxNum() % 5000 == 0) {
			System.out.println("Trying to avoid hot partition");
			System.out.println("Second most Read: " + secondMostPartId);
		}
		
		if (readDistribution[secondMostPartId] > 0)
			return secondMostPartId;
		
//		return PART_TO_AVOID + (int) (task.getTxNum() % (PartitionMetaMgr.NUM_PARTITIONS - 1) + 1);
		return mostReadPartId;
	}
}
