package org.elasql.perf.tpart.ai;

import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class ReadCountEstimator implements Estimator {
	
//	private static double[] latency = new double[] {10, 7.423, 4.765};
//	private static int[] masterCpuTime = new int[] {60, 90, 120};
//	private static int[] slaveCpuTime = new int[] {22, 52, 82};
	
	private static final int MAX_READ = 24;
	
	// TPC-C
	private static double[] latency;
	private static int[] masterCpuTime;
	private static int[] slaveCpuTime;
	
	static {
		latency = new double[MAX_READ];
		masterCpuTime = new int[MAX_READ];
		slaveCpuTime = new int[MAX_READ];
		
		for (int i = 0; i < MAX_READ; i++) {
			latency[MAX_READ - i - 1] = 50 + i * 5;
			masterCpuTime[i] = 200 + i * 50;
			slaveCpuTime[i] = 100 + i * 50;
		}
	}
	
	@Override
	public TransactionEstimation estimate(TransactionFeatures features) {
		TransactionEstimation.Builder builder = new TransactionEstimation.Builder();
		
		for (int masterId = 0; masterId < PartitionMetaMgr.NUM_PARTITIONS; masterId++)
			builder.setLatency(masterId, estimateLatency(features, masterId));

		for (int masterId = 0; masterId < PartitionMetaMgr.NUM_PARTITIONS; masterId++)
			builder.setMasterCpuCost(masterId, estimateMasterCpuCost(features, masterId));
		
		for (int slaveId = 0; slaveId < PartitionMetaMgr.NUM_PARTITIONS; slaveId++)
			builder.setSlaveCpuCost(slaveId, estimateSlaveCpuCost(features, slaveId));
		
		return builder.build();
	}

	@Override
	public void notifyTransactionRoute(long txNum, int masterId) {
		// Do nothing
	}
	
	private double estimateLatency(TransactionFeatures features, int masterId) {
		Integer[] readDistribution = (Integer[]) features.getFeature("Read Data Distribution");
		int readCount = readDistribution[masterId].intValue();
		return latency[readCount];
	}
	
	private long estimateMasterCpuCost(TransactionFeatures features, int masterId) {
		Integer[] readDistribution = (Integer[]) features.getFeature("Read Data Distribution");
		int readCount = readDistribution[masterId].intValue();
		return masterCpuTime[readCount];
	}
	
	private long estimateSlaveCpuCost(TransactionFeatures features, int slaveId) {
		Integer[] readDistribution = (Integer[]) features.getFeature("Read Data Distribution");
		int readCount = readDistribution[slaveId].intValue();
		return slaveCpuTime[readCount];
	}
}
