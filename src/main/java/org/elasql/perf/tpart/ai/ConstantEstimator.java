package org.elasql.perf.tpart.ai;

import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.storage.metadata.PartitionMetaMgr;

public class ConstantEstimator implements Estimator {
	
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
		return 0.3;
	}
	
	private long estimateMasterCpuCost(TransactionFeatures features, int masterId) {
		return 190;
	}
	
	private long estimateSlaveCpuCost(TransactionFeatures features, int slaveId) {
		return 90;
	}
}
