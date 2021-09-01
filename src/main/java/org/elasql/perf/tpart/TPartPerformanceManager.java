package org.elasql.perf.tpart;

import org.elasql.perf.MetricReport;
import org.elasql.perf.PerformanceManager;
import org.elasql.perf.tpart.ai.Estimator;
import org.elasql.perf.tpart.metric.MetricCollector;
import org.elasql.perf.tpart.metric.MetricWarehouse;
import org.elasql.perf.tpart.metric.TPartSystemMetrics;
import org.elasql.perf.tpart.workload.FeatureCollector;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.server.Elasql;
import org.vanilladb.core.util.TransactionProfiler;

public class TPartPerformanceManager implements PerformanceManager {

	// On the sequencer
	private FeatureCollector featureCollector;
	private MetricWarehouse metricWarehouse;
	
	// On each DB machine
	private MetricCollector localMetricCollector;
	
	public TPartPerformanceManager(TPartStoredProcedureFactory factory) {
		if (Estimator.ENABLE_COLLECTING_DATA) {
			if (Elasql.isStandAloneSequencer()) {
				// The sequencer maintains a feature collector and a warehouse
				featureCollector = new FeatureCollector(factory);
				Elasql.taskMgr().runTask(featureCollector);
				
				metricWarehouse = new MetricWarehouse();
				Elasql.taskMgr().runTask(metricWarehouse);
			} else {
				localMetricCollector = new MetricCollector();
				Elasql.taskMgr().runTask(localMetricCollector);
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
		metricWarehouse.receiveMetricReport((TPartSystemMetrics) report);
	}
	
	@Override
	public MetricWarehouse getMetricWarehouse() {
		return metricWarehouse;
	}
}
