package org.elasql.schedule.tpart.control;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.elasql.perf.tpart.ai.TransactionEstimation;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.PeriodicalJob;

public class ControlBasedRouter implements BatchNodeInserter {
	
	private static final double LATENCY_EXP = 1.0;
	private static final double TIE_CLOSENESS = 0.0001;

	// Debug: show the distribution of assign masters
	private static int[] assignedCounts;
	static {
		assignedCounts = new int[PartitionMetaMgr.NUM_PARTITIONS];
		new PeriodicalJob(5_000, 360_000, new Runnable() {
			@Override
			public void run() {
				long time = System.currentTimeMillis() - Elasql.START_TIME_MS;
				time /= 1000;
				
				StringBuffer sb = new StringBuffer();
				sb.append(String.format("Time: %d seconds - ", time));
				for (int i = 0; i < assignedCounts.length; i++) {
					sb.append(String.format("%d, ", assignedCounts[i]));
					assignedCounts[i] = 0;
				}
				sb.delete(sb.length() - 2, sb.length());
				
				System.out.println(sb.toString());
			}
		}).start();
	}
	
	private double[] paramAlpha;
	private double[] paramBeta;
	private double[] paramGamma;

	private List<Integer> ties = new ArrayList<Integer>();
	
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
		for (TPartStoredProcedureTask task : tasks)
			insert(graph, task);
	}

	private void insert(TGraph graph, TPartStoredProcedureTask task) {
		TransactionEstimation estimation = task.getEstimation();
		
		if (estimation == null)
			throw new IllegalArgumentException("there is no estimation for transaction " + task.getTxNum());
		
		int bestMasterId = 0;
		double highestScore = calculateRoutingScore(estimation, 0);
		ties.add(bestMasterId);
		
		for (int masterId = 1; masterId < PartitionMetaMgr.NUM_PARTITIONS; masterId++) {
			double score = calculateRoutingScore(estimation, masterId);
			if (score > highestScore) {
				bestMasterId = masterId;
				highestScore = score;
				ties.clear();
				ties.add(masterId);
			} else if (Math.abs(score - highestScore) < TIE_CLOSENESS) { // Ties
				ties.add(masterId);
			}
		}
		
		// Handles ties to avoid always sending txs to the same node
		if (ties.size() > 1) {
			int chooseTiePart = (int) (task.getTxNum() % ties.size());
			bestMasterId = ties.get(chooseTiePart);
		}
		
		// Debug
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
}
