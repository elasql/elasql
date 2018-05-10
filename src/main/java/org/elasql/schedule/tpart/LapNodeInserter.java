package org.elasql.schedule.tpart;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class LapNodeInserter extends CostAwareNodeInserter {
	
	private static class UseCount {
		
		private int count;
		
		UseCount(int count) {
			this.count = count;
		}
		
		int get() {
			return count;
		}
		
		void increment() {
			count++;
		}
		
		void decrement() {
			count--;
		}
	}
	
	private double[] loadPerPart = new double[PartitionMetaMgr.NUM_PARTITIONS];
	private Map<RecordKey, UseCount> useCounts = new HashMap<RecordKey, UseCount>();

	/**
	 * Insert this node to the partition that will result in minimal cost.
	 */
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		// Analyze the batch
		analyzeBatch(tasks);
		
		// Sequentially insert each node
		for (TPartStoredProcedureTask task : tasks) {
			insertNode(graph, task);
		}
		
		// Reset the statistics
		for (int partId = 0; partId < PartitionMetaMgr.NUM_PARTITIONS; partId++)
			loadPerPart[partId] = 0.0;
		useCounts.clear();
	}
	
	// Analyze batch for looking-ahead when calculate the costs
	private void analyzeBatch(List<TPartStoredProcedureTask> batch) {
		for (TPartStoredProcedureTask task : batch) {
			for (RecordKey readKey : task.getReadSet()) {
				UseCount count = useCounts.get(readKey);
				if (count == null) {
					count = new UseCount(0);
					useCounts.put(readKey, count);
				}
				count.increment();
			}
		}
	}
		
	private void insertNode(TGraph graph, TPartStoredProcedureTask task) {
		// Evaluate the cost on each part
		double minCost = Double.MAX_VALUE;
		int minCostPart = 0;
		
		for (int partId = 0; partId < PartitionMetaMgr.NUM_PARTITIONS; partId++) {
			double cost = estimateCost(graph, task, partId);
			if (cost < minCost) {
				minCost = cost;
				minCostPart = partId;
			}
		}
		
		// Insert the node
		graph.insertTxNode(task, minCostPart);
		
		// Update the statistics
		for (RecordKey key : task.getReadSet()) {
			UseCount count = useCounts.get(key);
			if (count == null) {
				throw new RuntimeException("We do not have use count for " + key);
			}
			count.decrement();
		}
		loadPerPart[minCostPart] += task.getWeight();
	}
	
	private double estimateCost(TGraph graph, TPartStoredProcedureTask task, int targetPart) {
		// calculate cross partition edge cost
		double crossEdgeCost = 0;
		for (RecordKey key : task.getReadSet()) {
			if (graph.getResourcePosition(key).getPartId() != targetPart) {
				UseCount count = useCounts.get(key);
				
				if (count == null) {
					throw new RuntimeException("We do not have use count for " + key);
				}
				
				crossEdgeCost += count.get();
			}
		}

		// calculate partition load cost
		double loadCost = loadPerPart[targetPart] + task.getWeight();

		return truncate(loadCost * (1 - BETA) + crossEdgeCost * BETA, 4);
	}
}
