package org.elasql.perf.tpart;

import org.elasql.perf.MetricReport;
import org.elasql.perf.MetricWarehouse;
import org.elasql.perf.PerformanceManager;
import org.elasql.perf.TransactionMetricReport;
import org.elasql.perf.tpart.ai.ConstantEstimator;
import org.elasql.perf.tpart.ai.Estimator;
import org.elasql.perf.tpart.ai.ReadCountEstimator;
import org.elasql.perf.tpart.bandit.RoutingBanditActuator;
import org.elasql.perf.tpart.bandit.data.BanditTransactionContextFactory;
import org.elasql.perf.tpart.bandit.data.BanditTransactionDataCollector;
import org.elasql.perf.tpart.bandit.data.BanditTransactionReward;
import org.elasql.perf.tpart.ai.SumMaxEstimator;
import org.elasql.perf.tpart.control.RoutingControlActuator;
import org.elasql.perf.tpart.metric.*;
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
				.getPropertyAsBoolean(TPartPerformanceManager.class.getName() + ".ENABLE_COLLECTING_DATA", false);
		ESTIMATOR_TYPE = ElasqlProperties.getLoader()
				.getPropertyAsInteger(TPartPerformanceManager.class.getName() + ".ESTIMATOR_TYPE", 0);
	}

	public static TPartPerformanceManager newForSequencer(TPartStoredProcedureFactory factory,
			BatchNodeInserter inserter, TGraph graph, boolean isBatching) {
		// The sequencer maintains a SpCallPreprocessor and a warehouse.
		TpartMetricWarehouse metricWarehouse = new TpartMetricWarehouse();
		Elasql.taskMgr().runTask(metricWarehouse);

		BanditTransactionDataCollector banditTransactionDataCollector = newBanditTransactionCollector();
		BanditTransactionContextFactory banditTransactionContextFactory = newBanditTransactionContextFactory();
		SpCallPreprocessor spCallPreprocessor = new SpCallPreprocessor(factory, inserter, graph, isBatching,
				metricWarehouse, newEstimator(), banditTransactionDataCollector, banditTransactionContextFactory);
		Elasql.taskMgr().runTask(spCallPreprocessor);

		// Hermes-Control has a control actuator
		RoutingControlActuator actuator = null;
		if (Elasql.SERVICE_TYPE == Elasql.ServiceType.HERMES_CONTROL) {
			actuator = new RoutingControlActuator(metricWarehouse);
			Elasql.taskMgr().runTask(actuator);
		} else if (Elasql.SERVICE_TYPE == Elasql.ServiceType.HERMES_BANDIT) {
			RoutingBanditActuator routingBanditActuator = new RoutingBanditActuator();
			Elasql.taskMgr().runTask(routingBanditActuator);
			return new TPartPerformanceManager(spCallPreprocessor, metricWarehouse, banditTransactionDataCollector, routingBanditActuator);
		}

		return new TPartPerformanceManager(spCallPreprocessor, metricWarehouse, actuator);
	}

	public static TPartPerformanceManager newForDbServer() {
		MetricCollector localMetricCollector = new MetricCollector();
		Elasql.taskMgr().runTask(localMetricCollector);

		return new TPartPerformanceManager(localMetricCollector);
	}

	private static Estimator newEstimator() {
		if (Elasql.SERVICE_TYPE != Elasql.ServiceType.HERMES_CONTROL) {
			return null;
		}

		switch (ESTIMATOR_TYPE) {
		case 0:
			return new ConstantEstimator();
		case 1:
			return new ReadCountEstimator();
		case 2:
			return new SumMaxEstimator();
		default:
			throw new IllegalArgumentException("Not supported");
		}
	}

	private static BanditTransactionDataCollector newBanditTransactionCollector() {
		if (Elasql.SERVICE_TYPE == Elasql.ServiceType.HERMES_BANDIT) {
			return new BanditTransactionDataCollector();
		}

		return null;
	}

	private static BanditTransactionContextFactory newBanditTransactionContextFactory() {
		if (Elasql.SERVICE_TYPE == Elasql.ServiceType.HERMES_BANDIT) {
			return new BanditTransactionContextFactory();
		}

		return null;
	}

	// On the sequencer
	private SpCallPreprocessor spCallPreprocessor;
	private TpartMetricWarehouse metricWarehouse;
	private RoutingControlActuator actuator;
	private BanditTransactionDataCollector banditTransactionDataCollector;
	private RoutingBanditActuator banditActuator;

	// On each DB machine
	private MetricCollector localMetricCollector;

	private TPartPerformanceManager(SpCallPreprocessor spCallPreprocessor, TpartMetricWarehouse metricWarehouse,
			RoutingControlActuator actuator) {
		this.spCallPreprocessor = spCallPreprocessor;
		this.metricWarehouse = metricWarehouse;
		this.actuator = actuator;
	}

	private TPartPerformanceManager(SpCallPreprocessor spCallPreprocessor, TpartMetricWarehouse metricWarehouse,
									BanditTransactionDataCollector banditTransactionDataCollector, RoutingBanditActuator banditActuator) {
		this.spCallPreprocessor = spCallPreprocessor;
		this.metricWarehouse = metricWarehouse;
		this.banditTransactionDataCollector = banditTransactionDataCollector;
		this.banditActuator = banditActuator;
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
	public void receiveTransactionMetricReport(TransactionMetricReport report) {
		if (banditTransactionDataCollector == null || banditActuator == null) {
			throw new RuntimeException("Cannot receive transaction metric report");
		}
		banditActuator.addTransactionData(banditTransactionDataCollector.addRewardAndTakeOut((BanditTransactionReward) report));
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
