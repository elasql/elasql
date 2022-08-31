package org.elasql.perf.tpart;

import org.elasql.perf.MetricReport;
import org.elasql.perf.MetricWarehouse;
import org.elasql.perf.PerformanceManager;
import org.elasql.perf.tpart.ai.ConstantEstimator;
import org.elasql.perf.tpart.ai.Estimator;
import org.elasql.perf.tpart.ai.ReadCountEstimator;
import org.elasql.perf.tpart.ai.SumMaxEstimator;
import org.elasql.perf.tpart.control.ControlParamUpdater;
import org.elasql.perf.tpart.control.PidControlAgent;
import org.elasql.perf.tpart.mdp.bandit.BanditAgent;
import org.elasql.perf.tpart.mdp.rl.agent.FullyOfflineAgent;
import org.elasql.perf.tpart.mdp.rl.agent.OfflineAgent;
import org.elasql.perf.tpart.mdp.rl.agent.OnlineAgent;
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
	public static final int RL_TYPE;

	static {
		ENABLE_COLLECTING_DATA = ElasqlProperties.getLoader()
				.getPropertyAsBoolean(TPartPerformanceManager.class.getName() + ".ENABLE_COLLECTING_DATA", false);
		ESTIMATOR_TYPE = ElasqlProperties.getLoader()
				.getPropertyAsInteger(TPartPerformanceManager.class.getName() + ".ESTIMATOR_TYPE", 0);
		RL_TYPE = ElasqlProperties.getLoader()
				.getPropertyAsInteger(TPartPerformanceManager.class.getName() + ".RL_TYPE", 2);
	}

	public static TPartPerformanceManager newForSequencer(TPartStoredProcedureFactory factory,
			BatchNodeInserter inserter, TGraph graph, boolean isBatching) {
		// The sequencer maintains a SpCallPreprocessor and a warehouse.
		TpartMetricWarehouse metricWarehouse = new TpartMetricWarehouse();
		Elasql.taskMgr().runTask(metricWarehouse);

		SpCallPreprocessor spCallPreprocessor = new SpCallPreprocessor(factory, inserter, graph, isBatching,
				metricWarehouse, newEstimator(), newRoutingAgent(metricWarehouse));

		Elasql.taskMgr().runTask(spCallPreprocessor);

		// Hermes-Control has a control actuator
		if (Elasql.SERVICE_TYPE == Elasql.ServiceType.HERMES_CONTROL) {
			ControlParamUpdater actuator = new ControlParamUpdater(metricWarehouse);
			Elasql.taskMgr().runTask(actuator);
		}

		return new TPartPerformanceManager(spCallPreprocessor, metricWarehouse);
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
	
	private static CentralRoutingAgent newRoutingAgent(TpartMetricWarehouse metricWarehouse) {
		switch (Elasql.SERVICE_TYPE) {
		case HERMES_CONTROL:
			return new PidControlAgent(metricWarehouse);
		case HERMES_RL:
			switch (RL_TYPE) {
			case 0:
				return new FullyOfflineAgent();
			case 1:
				return new OfflineAgent();
			case 2:
				return new OnlineAgent();
			default:
				throw new IllegalArgumentException("Not supported");
			}
		case HERMES_BANDIT_SEQUENCER:
			return new BanditAgent();
		default:
			return null;
		}
	}

	// On the sequencer
	private SpCallPreprocessor spCallPreprocessor;
	private TpartMetricWarehouse metricWarehouse;
	
	// On each DB machine
	private MetricCollector localMetricCollector;

	private TPartPerformanceManager(SpCallPreprocessor spCallPreprocessor, TpartMetricWarehouse metricWarehouse) {
		this.spCallPreprocessor = spCallPreprocessor;
		this.metricWarehouse = metricWarehouse;
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
	public void onTransactionCommit(long txNum, int masterId, long txLatency) {
		spCallPreprocessor.onTransactionCommit(txNum, masterId, txLatency);
	}
}
