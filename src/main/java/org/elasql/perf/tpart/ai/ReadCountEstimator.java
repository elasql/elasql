package org.elasql.perf.tpart.ai;

import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class ReadCountEstimator implements Estimator {
	
//	private static int[] masterCpuTime = new int[] {35, 123, 204};
//	private static int[] slaveCpuTime = new int[] {42, 130, 137};
	
	private static double[] latency = new double[] {10, 7.423, 4.765};
	
	private static int[] masterCpuTime = new int[] {60, 90, 120};
	private static int[] slaveCpuTime = new int[] {22, 52, 82};
	
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