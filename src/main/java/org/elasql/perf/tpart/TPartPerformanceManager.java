package org.elasql.perf.tpart;

import org.elasql.perf.MetricReport;
import org.elasql.perf.MetricWarehouse;
import org.elasql.perf.PerformanceManager;
import org.elasql.perf.tpart.ai.ConstantEstimator;
import org.elasql.perf.tpart.ai.Estimator;
import org.elasql.perf.tpart.ai.ReadCountEstimator;
import org.elasql.perf.tpart.ai.SumMaxEstimator;
import org.elasql.perf.tpart.control.RoutingControlActuator;
import org.elasql.perf.tpart.metric.MetricCollector;
import org.elasql.perf.tpart.metric.TPartSystemMetrics;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.perf.tpart.preprocessor.DropPreprocessor;
import org.elasql.perf.tpart.preprocessor.SlaPreprocessor;
import org.elasql.perf.tpart.preprocessor.SpCallPreprocessor;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.server.Elasql;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.core.util.TransactionProfiler;

import java.lang.reflect.Constructor;
import java.util.logging.Level;
import java.util.logging.Logger;

public class TPartPerformanceManager implements PerformanceManager {
	// Logger
	private static Logger logger = Logger.getLogger(TPartPerformanceManager.class.getName());

	public static final boolean ENABLE_COLLECTING_DATA;
	public static final int ESTIMATOR_TYPE;
	public static final int PREPROCESSOR_TYPE;
	public static double TRANSACTION_DEADLINE;
	public static double ESTIMATION_ERROR;
	public static Class<?> PreprocessorCls;

	static {
		PREPROCESSOR_TYPE = ElasqlProperties.getLoader()
				.getPropertyAsInteger(TPartPerformanceManager.class.getName() + ".PREPROCESSOR_TYPE", 0);
		ENABLE_COLLECTING_DATA = ElasqlProperties.getLoader()
				.getPropertyAsBoolean(TPartPerformanceManager.class.getName() + ".ENABLE_COLLECTING_DATA", false);
		ESTIMATOR_TYPE = ElasqlProperties.getLoader()
				.getPropertyAsInteger(TPartPerformanceManager.class.getName() + ".ESTIMATOR_TYPE", 2);
		TRANSACTION_DEADLINE = ElasqlProperties.getLoader()
				.getPropertyAsDouble(TPartPerformanceManager.class.getName() + ".TRANSACTION_DEADLINE",  12500);
		ESTIMATION_ERROR = ElasqlProperties.getLoader()
				.getPropertyAsDouble(TPartPerformanceManager.class.getName() + ".ESTIMATION_ERROR", 2500);
	}

	public static TPartPerformanceManager newForSequencer(TPartStoredProcedureFactory factory,
			BatchNodeInserter inserter, TGraph graph, boolean isBatching) {
		// The sequencer maintains a SpCallPreprocessor and a warehouse.
		TpartMetricWarehouse metricWarehouse = new TpartMetricWarehouse();
		Elasql.taskMgr().runTask(metricWarehouse);

		SpCallPreprocessor spCallPreprocessor = newPreprocessor(factory, inserter,
				graph, isBatching, metricWarehouse, newEstimator());

		if (logger.isLoggable(Level.INFO)) {
			logger.info(String.format("Using %s", spCallPreprocessor.getClass().getName()));
		}

		Elasql.taskMgr().runTask(spCallPreprocessor);

		// Hermes-Control has a control actuator
		RoutingControlActuator actuator = null;
		if (Elasql.SERVICE_TYPE == Elasql.ServiceType.HERMES_CONTROL) {
			actuator = new RoutingControlActuator(metricWarehouse);
			Elasql.taskMgr().runTask(actuator);
		}

		return new TPartPerformanceManager(spCallPreprocessor, metricWarehouse, actuator);
	}

	public static TPartPerformanceManager newForDbServer() {
		MetricCollector localMetricCollector = new MetricCollector();
		Elasql.taskMgr().runTask(localMetricCollector);

		return new TPartPerformanceManager(localMetricCollector);
	}

	private static Estimator newEstimator() {
		switch (ESTIMATOR_TYPE) {
		case 0:
			return new ConstantEstimator();
		case 1:
			return new ReadCountEstimator();
		case 2:
		    return new SumMaxEstimator();
		case 3:
			// Training mode
			return null;
		default:
			throw new IllegalArgumentException("Not supported");
		}
	}

	private static SpCallPreprocessor newPreprocessor(TPartStoredProcedureFactory factory,
													  BatchNodeInserter inserter,
													  TGraph graph,
													  boolean isBatching,
													  TpartMetricWarehouse metricWarehouse, Estimator estimator) {
		switch (PREPROCESSOR_TYPE) {
		case 0:
			return new SpCallPreprocessor(factory, inserter, graph, isBatching, metricWarehouse, estimator);
		case 1:
			return new SlaPreprocessor(factory, inserter, graph, isBatching, metricWarehouse, estimator);
		case 2:
			return new DropPreprocessor(factory, inserter, graph, isBatching, metricWarehouse, estimator);
		default:
			throw new IllegalArgumentException("Preprocessor not supported");
		}
	}

	// On the sequencer
	private SpCallPreprocessor spCallPreprocessor;
	private TpartMetricWarehouse metricWarehouse;
	private RoutingControlActuator actuator;

	// On each DB machine
	private MetricCollector localMetricCollector;

	private TPartPerformanceManager(SpCallPreprocessor spCallPreprocessor, TpartMetricWarehouse metricWarehouse,
			RoutingControlActuator actuator) {
		this.spCallPreprocessor = spCallPreprocessor;
		this.metricWarehouse = metricWarehouse;
		this.actuator = actuator;
	}

	private TPartPerformanceManager(MetricCollector localMetricCollector) {
		this.localMetricCollector = localMetricCollector;
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
	
	@Override
	public void onTransactionCommit(long txNum, int masterId) {
		spCallPreprocessor.onTransactionCommit(txNum, masterId);
	}
}
