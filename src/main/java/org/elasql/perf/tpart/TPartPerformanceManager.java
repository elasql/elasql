package org.elasql.perf.tpart;

import org.elasql.perf.MetricReport;
import org.elasql.perf.PerformanceManager;
import org.elasql.perf.tpart.ai.Estimator;
import org.elasql.perf.tpart.metric.MetricCollector;
import org.elasql.perf.tpart.workload.FeatureCollector;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.server.Elasql;
import org.vanilladb.core.util.TransactionProfiler;

public class TPartPerformanceManager implements PerformanceManager {

	// On the sequencer
	private FeatureCollector featureCollector;
	
	// On each DB machine
	private MetricCollector localMetricCollector;
	
	public TPartPerformanceManager(TPartStoredProcedureFactory factory) {
		if (Estimator.ENABLE_COLLECTING_DATA) {
			if (Elasql.isStandAloneSequencer()) {
				// The sequencer maintains a feature collector
				featureCollector = new FeatureCollector(factory);
				Elasql.taskMgr().runTask(featureCollector);
			} else {
				localMetricCollector = new MetricCollector();
			}
		}
	}

	@Override
	public void monitorTransaction(StoredProcedureCall spc) {
		if (Estimator.ENABLE_COLLECTING_DATA) {
			if (Elasql.isStandAloneSequencer()) {
				featureCollector.monitorTransaction(spc);
			}
		}
	}

	@Override
	public void addTransactionMetics(long txNum, String role, TransactionProfiler profiler) {
		if (Estimator.ENABLE_COLLECTING_DATA) {
			if (!Elasql.isStandAloneSequencer()) {
				localMetricCollector.addTransactionMetrics(txNum, role, profiler);
			}
		}
	}

	@Override
	public void receiveMetricReport(MetricReport report) {
		// TODO: store system metrics for cost estimation and PID control
	}
}
