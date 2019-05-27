package org.elasql.schedule.tpart;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class LapNodeInserter extends CostAwareNodeInserter {
	
	private static final double EQUALITY_THRESHOLD = .0001;
	
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
	private PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();

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
		for (int partId = 0; partId < partMgr.getCurrentNumOfParts(); partId++)
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
		// for scaling-out experiments
//		if (!isScalingOut && task.getTxNum() >= CHANGE_TX_NUM) {
//			isScalingOut = true;
//			System.out.println("Start scaling out at " + 
//					(System.currentTimeMillis() - Elasql.START_TIME_MS) + " ms");
//		}
		// for consolidation experiments
//		if (!isConsolidating && task.getTxNum() >= CHANGE_TX_NUM) {
//			isConsolidating = true;
//			System.out.println("Start consolidation at " + 
//					(System.currentTimeMillis() - Elasql.START_TIME_MS) + " ms");
//		}
		
		// Evaluate the cost on each part
		double[] costs = new double[partMgr.getCurrentNumOfParts()];
		Arrays.fill(costs, Double.MAX_VALUE);
		double minCost = Double.MAX_VALUE;
		int minCostPart = 0;
		
		for (int partId = 0; partId < partMgr.getCurrentNumOfParts(); partId++) {
			// for scaling-out experiments
//			if (!isScalingOut && partId > 2) 
//				break;
			// for consolidation experiments
//			if (isConsolidating && partId > 2)
//				break;
			
			costs[partId] = estimateCost(graph, task, partId);
			if (costs[partId] < minCost) {
				minCost = costs[partId];
				minCostPart = partId;
			}
		}
		
		// Handle ties
		int tieCount = 0; 
		for (int partId = 0; partId < partMgr.getCurrentNumOfParts(); partId++) {
			if (Math.abs(costs[partId] - minCost) < EQUALITY_THRESHOLD) {
				tieCount++;
			} 
		}
		
		if (tieCount > 1) {
			int chooseTiePart = (int) (task.getTxNum() % tieCount);
			tieCount = 0;
			for (int partId = 0; partId < partMgr.getCurrentNumOfParts(); partId++) {
				if (Math.abs(costs[partId] - minCost) < EQUALITY_THRESHOLD) {
					if (chooseTiePart == tieCount) {
						minCostPart = partId;
						break;
					} else {
						tieCount++;
					}
				}
			}
		}
		
		// Debug: Find the tie
//		printTieStatistics(graph, task, minCostPart, minCost);
		
//		if (task.getTxNum() % 10000 == 0)
//			System.out.println("Tx." + task.getTxNum() + " select " + minCostPart);
		
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
	
	private long nextReportTime = 0;
	private int[] tieCounts = new int[PartitionMetaMgr.NUM_PARTITIONS];
	
	private void printTieStatistics(TGraph graph, TPartStoredProcedureTask task, int finalPart, double finalCost) {
		long time = (System.currentTimeMillis() - Elasql.START_TIME_MS) / 1000;
		boolean hasTie = false;
		
		// Find the tie
		for (int partId = 0; partId < partMgr.getCurrentNumOfParts(); partId++) {
			double cost = estimateCost(graph, task, partId);
			if (Math.abs(cost - finalCost) < EQUALITY_THRESHOLD) {
				if (partId != finalPart) {
					hasTie = true;
//					int mod = (int) (task.getTxNum() % partMgr.getCurrentNumOfParts());
//					System.out.println(String.format(
//							"Tx.%d chooses node.%d (%f) instead of node.%d (%f) since it's close to %d",
//							task.getTxNum(), finalPart, finalCost, partId, cost, mod));
				}
			}
		}
		
		if (hasTie)
			tieCounts[finalPart]++;
		
		// prints every period of time
		if (time >= nextReportTime) {
			System.out.println(String.format("Time: %d", time));
			
			System.out.println(String.format("Number of ties: %s.",
					Arrays.toString(tieCounts)));
			Arrays.fill(tieCounts, 0);
			
			nextReportTime = time + 5;
		}
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
