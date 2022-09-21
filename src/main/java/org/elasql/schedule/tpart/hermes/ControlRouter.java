package org.elasql.schedule.tpart.hermes;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

import org.elasql.perf.tpart.control.PidController;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class ControlRouter implements BatchNodeInserter {
	
	private static final double TIE_CLOSENESS = 1.0; // 1%. Note that scores are in 0 ~ 1
	private static final long CONTROL_UPDATE_PERIOD = 100; // in milliseconds
	private static final int WINDOW_SIZE = 10; // PERIOD * WINDOW_SIZE
	
	private PartitionMetaMgr partMgr = Elasql.partitionMetaMgr();
	private List<Integer> ties = new ArrayList<Integer>();
	
	// Control parameters
	private long lastUpdateTime = -1;
	private double[] recentLoads;
	private Queue<double[]> loadHistory;
	private PidController[] alphaControllers;
	private double[] paramAlpha;
	
	// Debug: show control update log
	private long lastPrintDebugTime = -1;
	
	// Debug: show the distribution of assigned masters
	private long lastReportTime = -1;
	private int[] assignedCounts = new int[PartitionMetaMgr.NUM_PARTITIONS];
	
	public ControlRouter() {
		recentLoads = new double[PartitionMetaMgr.NUM_PARTITIONS];
		loadHistory = new ArrayDeque<double[]>();
		alphaControllers = new PidController[PartitionMetaMgr.NUM_PARTITIONS];
		paramAlpha = new double[PartitionMetaMgr.NUM_PARTITIONS];
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
			alphaControllers[nodeId] = new PidController(1.0);
			paramAlpha[nodeId] = 1.0;
		}
	}

	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			updateControlParameters(task);
			
			insert(graph, task);

			// Debug: show the distribution of assigned masters
			reportRoutingDistribution(task.getArrivedTime());
		}
	}

	@Override
	public boolean needBatching() {
		return false;
	}
	
	private void updateControlParameters(TPartStoredProcedureTask task) {
		if (lastUpdateTime == -1) {
			lastUpdateTime = task.getArrivedTime();
			lastPrintDebugTime = task.getArrivedTime();
			return;
		}
		
		long elapsedTime = (task.getArrivedTime() - lastUpdateTime) / 1000;
		if (elapsedTime > CONTROL_UPDATE_PERIOD) {
			
			// Update each parameter via PID controllers
			double[] obs = getObservations();
			for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
				alphaControllers[nodeId].setObservation(obs[nodeId]);
				alphaControllers[nodeId].setReference(
						1.0 / PartitionMetaMgr.NUM_PARTITIONS);
				alphaControllers[nodeId].updateControlParameters(
						((double) elapsedTime) / 1000);
				paramAlpha[nodeId] = alphaControllers[nodeId].getControlParameter();
				
			}
			
			lastUpdateTime = task.getArrivedTime();
		}
		
		// Debug: recent update log
		if (task.getArrivedTime() - lastPrintDebugTime > 1000_000) {
			System.out.println(String.format("===== Alpha Status (at %d seconds) =====",
					task.getArrivedTime() / 1000_000));
			for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++)
				System.out.println("Alpha #" + nodeId + ": " + 
					alphaControllers[nodeId].getLatestUpdateLog());
			lastPrintDebugTime = task.getArrivedTime();
		}
	}
	
	private double[] getObservations() {
		// Add the recent loads
		loadHistory.add(recentLoads);
		recentLoads = new double[PartitionMetaMgr.NUM_PARTITIONS];
		
		// Ensure the window size
		if (loadHistory.size() > WINDOW_SIZE)
			loadHistory.remove();
		
		// Sums the loads
		double total = 0.0;
		double[] observations = new double[PartitionMetaMgr.NUM_PARTITIONS];
		for (double[] loads : loadHistory) {
			for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
				observations[nodeId] += loads[nodeId];
				total += loads[nodeId];
			}
		}
		
		// Normalize the observations
		for (int i = 0; i < recentLoads.length; i++) {
			observations[i] /= total;
		}
		
		return observations;
	}
	
	private void insert(TGraph graph, TPartStoredProcedureTask task) {
		// Calculate score for each node
		double[] scores = new double[PartitionMetaMgr.NUM_PARTITIONS];
		for (int masterId = 0; masterId < PartitionMetaMgr.NUM_PARTITIONS; masterId++) {
			scores[masterId] = calculateRoutingScore(graph, task, masterId);
		}
		
		// Select the node with highest score and check ties
		int bestMasterId = findBestMaster(scores, task.getTxNum());
		
		// Record the load
		recentLoads[bestMasterId] += task.getWeight();
		
		// Debug
		assignedCounts[bestMasterId]++;
		
		graph.insertTxNode(task, bestMasterId);
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
