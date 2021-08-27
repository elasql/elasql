package org.elasql.perf;

import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.vanilladb.core.util.Timer;

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
	public void addTransactionMetics(long txNum, String role, Timer timer) {
		// Do nothing
	}

	@Override
	public void receiveMetricReport(MetricReport report) {
		// Do nothing
	}
}
