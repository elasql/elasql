package org.elasql.perf.tpart;

import org.elasql.perf.MetricReport;
import org.elasql.perf.MetricWarehouse;
import org.elasql.perf.PerformanceManager;
import org.elasql.perf.tpart.ai.Estimator;
import org.elasql.perf.tpart.metric.MetricCollector;
import org.elasql.perf.tpart.metric.TPartSystemMetrics;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.perf.tpart.workload.FeatureCollector;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.core.util.TransactionProfiler;

public class TPartPerformanceManager implements PerformanceManager {
	
	public static final boolean ENABLE_COLLECTING_DATA;

	static {
		ENABLE_COLLECTING_DATA = ElasqlProperties.getLoader()
				.getPropertyAsBoolean(Estimator.class.getName() + ".ENABLE_COLLECTING_DATA", false);
	}

	// On the sequencer
	private FeatureCollector featureCollector;
	private TpartMetricWarehouse metricWarehouse;
	
	// On each DB machine
	private MetricCollector localMetricCollector;
	
	public TPartPerformanceManager(TPartStoredProcedureFactory factory, 
			BatchNodeInserter inserter, TGraph graph,
			boolean isBatching) {
		if (ENABLE_COLLECTING_DATA) {
			if (Elasql.isStandAloneSequencer()) {
				metricWarehouse = new TpartMetricWarehouse();
				Elasql.taskMgr().runTask(metricWarehouse);
				
				// The sequencer maintains a feature collector and a warehouse
				featureCollector = new FeatureCollector(factory, inserter,
						graph, isBatching, metricWarehouse);
				Elasql.taskMgr().runTask(featureCollector);
			} else {
				localMetricCollector = new MetricCollector();
				Elasql.taskMgr().runTask(localMetricCollector);
			}
		}
	} 

	@Override
	public void monitorTransaction(StoredProcedureCall spc) {
		if (ENABLE_COLLECTING_DATA) {
			if (Elasql.isStandAloneSequencer()) {
				featureCollector.monitorTransaction(spc);
			}
		}
	}

	@Override
	public void addTransactionMetics(long txNum, String role, TransactionProfiler profiler) {
		if (ENABLE_COLLECTING_DATA) {
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
