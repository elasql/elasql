package org.elasql.perf;

import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.core.util.TransactionProfiler;

/**
 * A placeholder for the systems that does not have an implementation
 * of {@code PerformanceManager}.
 * 
 * @author Yu-Shan Lin
 */
public class DummyPerformanceManager implements PerformanceManager {
	@Override
	public void monitorTransaction(StoredProcedureCall spc) {
		// Do nothing
	}

	@Override
	public void addTransactionMetics(long txNum, String role, TransactionProfiler profiler) {
		// Do nothing
	}

	@Override
	public void receiveMetricReport(MetricReport report) {
		// Do nothing
	}
	
	@Override
	public MetricWarehouse getMetricWarehouse() {
		throw new RuntimeException("Invalid function call on a dummy performance manager");
	}
}
