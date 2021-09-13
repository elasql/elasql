package org.elasql.perf.tpart.ai;

import org.elasql.perf.tpart.workload.TransactionFeatures;

public interface Estimator {
	
	double estimateLatency(TransactionFeatures features, int masterId);
	
	long estimateMasterCpuCost(TransactionFeatures features, int masterId);
	
	long estimateSlaveCpuCost(TransactionFeatures features, int slaveId);
	
}
