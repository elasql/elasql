package org.elasql.schedule.tpart.control;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class Version1DmdRouter implements BatchNodeInserter {
	
	private static final double TIE_CLOSENESS = 1.0; // 1%. Note that scores are in 0 ~ 1
	
	private PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
	private List<Integer> ties = new ArrayList<Integer>();
	
	// Control parameters
	private double[] paramAlpha;
	private double loadSum = 0;
	private int txCount = 0;
	private double eta = 1.0; // learning rate
	
	// Debug: show the distribution of assigned masters
	private long lastReportTime = -1;
	private int[] assignedCounts = new int[PartitionMetaMgr.NUM_PARTITIONS];
	
	public Version1DmdRouter() {
		paramAlpha = new double[PartitionMetaMgr.NUM_PARTITIONS];
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
			paramAlpha[nodeId] = 0.0;
		}
	}

	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			insert(graph, task);

			// Debug: show the distribution of assigned masters
			reportRoutingDistribution(task.getArrivedTime());
		}
	}
	
	private void insert(TGraph graph, TPartStoredProcedureTask task) {
		// Calculate score for each node
		double[] scores = new double[PartitionMetaMgr.NUM_PARTITIONS];
		for (int masterId = 0; masterId < PartitionMetaMgr.NUM_PARTITIONS; masterId++) {
			scores[masterId] = calculateRoutingScore(graph, task, masterId);
		}
		
		// Select the node with highest score and check ties
		int bestMasterId = findBestMaster(scores, task.getTxNum());
		
		// Update the parameter
		updateParameter(bestMasterId, task.getWeight());
		
		// Debug
		assignedCounts[bestMasterId]++;
//		if (task.getTxNum() % 10000 == 0)
//			System.out.println(String.format("Tx.%d's alpha: %s", task.getTxNum(), Arrays.toString(paramAlpha)));
		
		graph.insertTxNode(task, bestMasterId);
	}
	
	private void updateParameter(int decision, double load) {
		// Update average load
		loadSum += load;
		txCount++;
		double avgLoadBudget = loadSum / txCount / PartitionMetaMgr.NUM_PARTITIONS;
		
		// Update learning rate
		eta = 1 / Math.sqrt(txCount);
//		eta = 1 / Math.sqrt(100000);
		
		// Calculate gradient
		double[] gradient = new double[PartitionMetaMgr.NUM_PARTITIONS];
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
			if (decision == nodeId) {
				gradient[nodeId] = -load + avgLoadBudget;
			} else {
				gradient[nodeId] = avgLoadBudget;
			}
		}
		
		// Performs projected gradient descent
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
			paramAlpha[nodeId] -= eta * gradient[nodeId];
			paramAlpha[nodeId] = Math.max(0.0, paramAlpha[nodeId]);
		}
	}
	
	private double calculateRoutingScore(TGraph graph, TPartStoredProcedureTask task, int masterId) {
		double remoteReads = countRemoteReadEdge(graph, task, masterId);
		double load = task.getWeight();
		double score = -(remoteReads + paramAlpha[masterId] * load);
		return score;
	}
	
	private int countRemoteReadEdge(TGraph graph, TPartStoredProcedureTask task, int masterId) {
		int remoteEdgeCount = 0;
		
		for (PrimaryKey key : task.getReadSet()) {
			// Skip replicated records
			if (partMgr.isFullyReplicated(key))
				continue;
			
			if (graph.getResourcePosition(key).getPartId() != masterId) {
				remoteEdgeCount++;
			}
		}
		
		return remoteEdgeCount;
	}
	
	private int findBestMaster(double[] scores, long txNum) {
		int bestMasterId = 0;
		double highestScore = scores[bestMasterId];
		ties.clear();
		ties.add(bestMasterId);
		
		for (int masterId = 1; masterId < PartitionMetaMgr.NUM_PARTITIONS; masterId++) {
			if (Math.abs(scores[masterId] - highestScore) < TIE_CLOSENESS) { // Ties
				ties.add(masterId);
			} else if (scores[masterId] > highestScore) {
				bestMasterId = masterId;
				highestScore = scores[masterId];
				ties.clear();
				ties.add(masterId);
			}
		}
		
		// Handles ties to avoid always sending txs to the same node
		if (ties.size() > 1) {
			int chooseTiePart = (int) (txNum % ties.size());
			bestMasterId = ties.get(chooseTiePart);
		}
		
		return bestMasterId;
	}

	// Debug: show the distribution of assigned masters
	private void reportRoutingDistribution(long currentTime) {
		if (lastReportTime == -1) {
			lastReportTime = currentTime;
		} else if (currentTime - lastReportTime > 1_000_000) {
			StringBuffer sb = new StringBuffer();
			
			sb.append(String.format("Time: %d seconds - Routing: [%d",
					currentTime / 1_000_000, assignedCounts[0]));
			assignedCounts[0] = 0;
			for (int i = 1; i < assignedCounts.length; i++) {
				sb.append(String.format(", %d", assignedCounts[i]));
				assignedCounts[i] = 0;
			}
			
			sb.append(String.format("], Alpha: [%.2f", paramAlpha[0]));
			for (int i = 1; i < paramAlpha.length; i++)
				sb.append(String.format(", %.2f", paramAlpha[i]));
			sb.append("]");
			
			System.out.println(sb.toString());
			
			lastReportTime = currentTime;
		}
	}
}
