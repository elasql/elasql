package org.elasql.schedule.tpart.control;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.perf.tpart.ai.TransactionEstimation;
import org.elasql.perf.tpart.control.ControlParamUpdateParamHelper;
import org.elasql.perf.tpart.control.ControlParamUpdateProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class ControlBasedRouter implements BatchNodeInserter {
	private static Logger logger = Logger.getLogger(ControlBasedRouter.class.getName());
	
	private static final double LATENCY_EXP = 1.0;
	private static final double TIE_CLOSENESS = 0.0001;
	
	private double[] paramAlpha;
	private double[] paramBeta;
	private double[] paramGamma;

	private List<Integer> ties = new ArrayList<Integer>();

	// Debug: show the distribution of assigned masters
	private long lastReportTime = -1;
	private int[] assignedCounts = new int[PartitionMetaMgr.NUM_PARTITIONS];
	
	public ControlBasedRouter() {
		paramAlpha = new double[PartitionMetaMgr.NUM_PARTITIONS];
		Arrays.fill(paramAlpha, 1.0);
		paramBeta = new double[PartitionMetaMgr.NUM_PARTITIONS];
		Arrays.fill(paramBeta, 1.0);
		paramGamma = new double[PartitionMetaMgr.NUM_PARTITIONS];
		Arrays.fill(paramGamma, 1.0);
	}

	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			if (task.getProcedure().getClass().equals(ControlParamUpdateProcedure.class)) {
				ControlParamUpdateProcedure procedure = 
						(ControlParamUpdateProcedure) task.getProcedure();
				updateParameters(procedure.getParamHelper());
			} else {
				insert(graph, task);

				// Debug: show the distribution of assigned masters
				reportRoutingDistribution(task.getArrivedTime());
			}
		}
	}
	
	private void updateParameters(ControlParamUpdateParamHelper paramHelper) {
		for (int nodeId = 0; nodeId < PartitionMetaMgr.NUM_PARTITIONS; nodeId++) {
			paramAlpha[nodeId] = paramHelper.getAlpha(nodeId);
			paramBeta[nodeId] = paramHelper.getBeta(nodeId);
			paramGamma[nodeId] = paramHelper.getGamma(nodeId);
		}
		
		if (logger.isLoggable(Level.INFO))
			logger.info(String.format("updating routing paramters, alpha: %s, beta: %s, gamma: %s.",
					Arrays.toString(paramAlpha), Arrays.toString(paramBeta), Arrays.toString(paramGamma)));
	}

	private void insert(TGraph graph, TPartStoredProcedureTask task) {
		TransactionEstimation estimation = task.getEstimation();
		
		if (estimation == null)
			throw new IllegalArgumentException("there is no estimation for transaction " + task.getTxNum());

		int bestMasterId = 0;
		double highestScore = calculateRoutingScore(estimation, 0);
		ties.clear();
		ties.add(bestMasterId);
		
		for (int masterId = 1; masterId < PartitionMetaMgr.NUM_PARTITIONS; masterId++) {
			double score = calculateRoutingScore(estimation, masterId);
			
			if (Math.abs(score - highestScore) < TIE_CLOSENESS) { // Ties
				ties.add(masterId);
			} else if (score > highestScore) {
				bestMasterId = masterId;
				highestScore = score;
				ties.clear();
				ties.add(masterId);
			}
		}
		
		// Handles ties to avoid always sending txs to the same node
		if (ties.size() > 1) {
			int chooseTiePart = (int) (task.getTxNum() % ties.size());
			bestMasterId = ties.get(chooseTiePart);
		}
		
		// Debug
		if (isPartition0Tx(task))
			assignedCounts[bestMasterId]++;
		
		graph.insertTxNode(task, bestMasterId);
	}
	
	private double calculateRoutingScore(TransactionEstimation estimation, int masterId) {
		double latency = estimation.estimateLatency(masterId);
		double e = Math.pow(1 / latency, LATENCY_EXP);
		double cpuFactor = estimation.estimateMasterCpuCost(masterId) -
				estimation.estimateSlaveCpuCost(masterId);
		// TODO: Disk I/O Factor and Network I/O Factor
		return e - paramAlpha[masterId] * cpuFactor;
	}
	
	private boolean isPartition0Tx(TPartStoredProcedureTask task) {
		// Find the warehouse record and check w_id
		for (PrimaryKey key : task.getReadSet()) {
			if (key.getTableName().equals("warehouse")) {
				int wid = (Integer) key.getVal("w_id").asJavaVal();
				if (wid <= 10) {
					return true;
				} else {
					return false;
				}
			}
		}
		
		throw new RuntimeException("Something wrong");
	}

	// Debug: show the distribution of assigned masters
	private void reportRoutingDistribution(long currentTime) {
		if (lastReportTime == -1) {
			lastReportTime = currentTime;
		} else if (currentTime - lastReportTime > 5_000_000) {
			StringBuffer sb = new StringBuffer();
			
			sb.append(String.format("Time: %d seconds - Routing: ", currentTime / 1_000_000));
			for (int i = 0; i < assignedCounts.length; i++) {
				sb.append(String.format("%d, ", assignedCounts[i]));
				assignedCounts[i] = 0;
			}
			sb.delete(sb.length() - 2, sb.length());
			
			System.out.println(sb.toString());
			
			lastReportTime = currentTime;
		}
	}
}
