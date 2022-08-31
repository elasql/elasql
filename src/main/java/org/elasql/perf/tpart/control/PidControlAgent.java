package org.elasql.perf.tpart.control;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.perf.tpart.CentralRoutingAgent;
import org.elasql.perf.tpart.ai.TransactionEstimation;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.Route;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class PidControlAgent implements CentralRoutingAgent {
	private static Logger logger = Logger.getLogger(PidControlAgent.class.getName());
	
	private static final double LATENCY_EXP = 1.0;
	private static final double TIE_CLOSENESS = 0.01; // 1%. Note that scores are in 0 ~ 1
	

	private List<Integer> ties = new ArrayList<Integer>();

	private ControlParameters controlParams;
	private ControlParamUpdater paramUpdater;
	
	public PidControlAgent(TpartMetricWarehouse metricWarehouse) {
		controlParams = new ControlParameters();
		paramUpdater = new ControlParamUpdater(metricWarehouse);
		Elasql.taskMgr().runTask(paramUpdater);
	}

	@Override
	public Route suggestRoute(TGraph graph, TPartStoredProcedureTask task, TpartMetricWarehouse metricWarehouse) {
		checkForParamUpdate();
		int route = routeTransaction(task);
		return new Route(route);
	}

	@Override
	public void onTxRouted(long txNum, int routeDest) {
		// do nothing
	}

	@Override
	public void onTxCommitted(long txNum, int masterId, long latency) {
		// do nothing
	}
	
	private void checkForParamUpdate() {
		ControlParameters params = paramUpdater.acquireUpdate();
		
		if (params != null) {
			if (logger.isLoggable(Level.INFO))
				logger.info(String.format("updating routing paramters: %s", params));
			controlParams = params;
		}
	}
	
	private int routeTransaction(TPartStoredProcedureTask task) {
		TransactionEstimation estimation = task.getEstimation();
		
		if (estimation == null)
			throw new IllegalArgumentException("there is no estimation for transaction " + task.getTxNum());
		
		// Calculate score for each node
		double[] scores = new double[PartitionMetaMgr.NUM_PARTITIONS];
		for (int masterId = 0; masterId < PartitionMetaMgr.NUM_PARTITIONS; masterId++) {
			scores[masterId] = calculateRoutingScore(estimation, masterId);
		}
		
		if (task.getTxNum() % 10000 == 0) {
			System.out.println(String.format("Tx.%d's estimation: %s", task.getTxNum(), estimation));
			System.out.println(String.format("Tx.%d's scores: %s", task.getTxNum(), Arrays.toString(scores)));
		}
		
		// Normalize the score to 0 ~ 1 (for checking ties)
		normalizeScores(scores);
		
		// Select the node with highest score and check ties
		int bestMasterId = findBestMaster(scores, task.getTxNum());
		
		// Debug
		if (task.getTxNum() % 10000 == 0) {
			System.out.println(String.format("Tx.%d's scores (normalized): %s", task.getTxNum(), Arrays.toString(scores)));
		}
		
		return bestMasterId;
	}
	
	private double calculateRoutingScore(TransactionEstimation estimation, int masterId) {
		double latency = estimation.estimateLatency(masterId);
		double e = Math.pow(1 / latency, LATENCY_EXP);
		double cpuFactor = estimation.estimateMasterCpuCost(masterId) -
				estimation.estimateSlaveCpuCost(masterId);
		// TODO: Disk I/O Factor and Network I/O Factor
		return e - controlParams.alpha(masterId) * cpuFactor;
	}
	
	private void normalizeScores(double[] scores) {
		double min = Double.MAX_VALUE;
		double max = Double.MIN_VALUE;
		
		for (double score : scores) {
			if (min > score) {
				min = score;
			}
			if (max < score) {
				max = score;
			}
		}
		
		double scale = max - min;
		for (int i = 0; i < scores.length; i++) {
			scores[i] = (scores[i] - min) / scale; 
		}
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
}
