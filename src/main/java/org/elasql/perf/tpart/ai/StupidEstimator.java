package org.elasql.perf.tpart.ai;

import org.elasql.perf.tpart.workload.TransactionFeatures;

public class StupidEstimator implements Estimator {
	
	@Override
	public double estimateLatency(TransactionFeatures features, int masterId) {
		return 10.0;
	}
	
	@Override
	public long estimateMasterCpuCost(TransactionFeatures features, int masterId) {
		return 1000;
	}
	
	@Override
	public long estimateSlaveCpuCost(TransactionFeatures features, int slaveId) {
		return 500;
	}

}
