package org.elasql.schedule.tpart.control;

import java.util.Arrays;
import java.util.List;

import org.elasql.perf.tpart.ai.TransactionEstimation;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class ControlBasedRouter implements BatchNodeInserter {
	
	private static final double LATENCY_EXP = 1.0;
	
	private double[] paramAlpha;
	private double[] paramBeta;
	private double[] paramGamma;
	
	public ControlBasedRouter() {
		paramAlpha = new double[PartitionMetaMgr.NUM_PARTITIONS];
		Arrays.fill(paramAlpha, 1.0);
		paramBeta = new double[PartitionMetaMgr.NUM_PARTITIONS];
		Arrays.fill(paramBeta, 1.0);
		paramGamma = new double[PartitionMetaMgr.NUM_PARTITIONS];
		Arrays.fill(paramGamma, 1.0);
	}

	public void insert(TGraph graph, TPartStoredProcedureTask task) {
		TransactionEstimation estimation = task.getEstimation(); 
		int bestMasterId = 0;
		double highestScore = calculateRoutingScore(estimation, 0);
		
		for (int masterId = 1; masterId < PartitionMetaMgr.NUM_PARTITIONS; masterId++) {
			double score = calculateRoutingScore(estimation, masterId);
			if (score > highestScore) {
				bestMasterId = masterId;
				highestScore = score;
			}
		}
		
		graph.insertTxNode(task, bestMasterId);
	}

	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks)
			insert(graph, task);
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
