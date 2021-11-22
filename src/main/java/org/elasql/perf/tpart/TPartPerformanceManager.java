package org.elasql.perf.tpart;

import org.elasql.perf.MetricReport;
import org.elasql.perf.MetricWarehouse;
import org.elasql.perf.PerformanceManager;
import org.elasql.perf.tpart.ai.ConstantEstimator;
import org.elasql.perf.tpart.ai.Estimator;
import org.elasql.perf.tpart.ai.ReadCountEstimator;
import org.elasql.perf.tpart.control.RoutingControlActuator;
import org.elasql.perf.tpart.metric.MetricCollector;
import org.elasql.perf.tpart.metric.TPartSystemMetrics;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.core.util.TransactionProfiler;

public class TPartPerformanceManager implements PerformanceManager {
	
	public static final boolean ENABLE_COLLECTING_DATA;
	public static final int ESTIMATOR_TYPE;

	static {
		ENABLE_COLLECTING_DATA = ElasqlProperties.getLoader()
				.getPropertyAsBoolean(TPartPerformanceManager.class.getName()
						+ ".ENABLE_COLLECTING_DATA", false);
		ESTIMATOR_TYPE = ElasqlProperties.getLoader().getPropertyAsInteger( 
				TPartPerformanceManager.class.getName() + ".ESTIMATOR_TYPE", 0);
	}

	// On the sequencer
	private SpCallPreprocessor spCallPreprocessor;
	private TpartMetricWarehouse metricWarehouse;
	
	// On each DB machine
	private MetricCollector localMetricCollector;
	
	public TPartPerformanceManager(TPartStoredProcedureFactory factory, 
			BatchNodeInserter inserter, TGraph graph,
			boolean isBatching) {
		if (Elasql.isStandAloneSequencer()) {
			// The sequencer maintains a SpCallPreprocessor and a warehouse.
			metricWarehouse = new TpartMetricWarehouse();
			Elasql.taskMgr().runTask(metricWarehouse);
			
			spCallPreprocessor = new SpCallPreprocessor(factory, inserter,
					graph, isBatching, metricWarehouse, getEstimator());
			Elasql.taskMgr().runTask(spCallPreprocessor);
			
			// Hermes-Control has a control actuator
			if (Elasql.SERVICE_TYPE == Elasql.ServiceType.HERMES_CONTROL) {
				Elasql.taskMgr().runTask(new RoutingControlActuator(metricWarehouse));
			}
		} else {
			localMetricCollector = new MetricCollector();
			Elasql.taskMgr().runTask(localMetricCollector);
		}
	} 

	@Override
	public void preprocessSpCall(StoredProcedureCall spc) {
		if (Elasql.isStandAloneSequencer()) {
			spCallPreprocessor.preprocessSpCall(spc);
		}
	}

	@Override
	public void addTransactionMetics(long txNum, String role, boolean isTxDistributed, TransactionProfiler profiler) {
		if (ENABLE_COLLECTING_DATA) {
			if (!Elasql.isStandAloneSequencer()) {
				localMetricCollector.addTransactionMetrics(txNum, role, isTxDistributed, profiler);
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
	
	private Estimator getEstimator() {
		if (Elasql.SERVICE_TYPE != Elasql.ServiceType.HERMES_CONTROL) {
			return null;
		}
		
		switch (ESTIMATOR_TYPE) {
		case 0:
			return new ConstantEstimator();
		case 1:	
			return new ReadCountEstimator();
		default: 
			throw new IllegalArgumentException("Not supported");
		}
	}
}
