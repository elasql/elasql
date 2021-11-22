package org.elasql.perf.tpart.ai;

import org.elasql.perf.tpart.workload.TransactionFeatures;

public interface Estimator {
	
	TransactionEstimation estimate(TransactionFeatures features);
	
}
