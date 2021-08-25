package org.elasql.perf.tpart;

import org.elasql.perf.MetricReport;
import org.elasql.perf.PerformanceManager;
import org.elasql.perf.tpart.ai.Estimator;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.server.Elasql;
import org.vanilladb.core.util.Timer;

public class TPartPerformanceManager implements PerformanceManager {

	// On the sequencer
	private FeatureCollector featureCollector;
	private MetricWarehouse metricWarehouse;
	
	// On each DB machine
	private LocalMetricCollector localMetricCollector;
	
	public TPartPerformanceManager(TPartStoredProcedureFactory factory) {
		if (Estimator.ENABLE_COLLECTING_DATA) {
			if (Elasql.isStandAloneSequencer()) {
				// The sequencer maintains a feature collector
				featureCollector = new FeatureCollector(factory);
				Elasql.taskMgr().runTask(featureCollector);
			} else {
				localMetricCollector = new LocalMetricCollector();
				Elasql.taskMgr().runTask(localMetricCollector);
			}
		}
		
		metricWarehouse = new MetricWarehouse();
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
	public void addTransactionMetics(long txNum, String role, Timer timer) {
		if (Estimator.ENABLE_COLLECTING_DATA) {
			if (!Elasql.isStandAloneSequencer()) {
				localMetricCollector.addTransactionMetrics(txNum, role, timer);
			}
		}
	}

	@Override
	public void receiveMetricReport(MetricReport report) {
		metricWarehouse.receiveMetricReport((TPartMetricReport) report);
	}
}
