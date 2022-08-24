package org.elasql.perf.tpart;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.elasql.perf.tpart.ai.Estimator;
import org.elasql.perf.tpart.ai.TransactionEstimation;
import org.elasql.perf.tpart.bandit.RoutingBanditActuator;
import org.elasql.perf.tpart.bandit.data.*;
import org.elasql.perf.tpart.bandit.model.BanditModel;
import org.elasql.perf.tpart.bandit.data.BanditTransactionContext;
import org.elasql.perf.tpart.bandit.data.BanditTransactionContextFactory;
import org.elasql.perf.tpart.bandit.data.BanditTransactionDataCollector;
import org.elasql.perf.tpart.metric.TpartMetricWarehouse;
import org.elasql.perf.tpart.workload.FeatureExtractor;
import org.elasql.perf.tpart.workload.TransactionDependencyRecorder;
import org.elasql.perf.tpart.workload.TransactionFeatures;
import org.elasql.perf.tpart.workload.TransactionFeaturesRecorder;
import org.elasql.perf.tpart.workload.time.TimeRelatedFeatureMgr;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedure.ProcedureType;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.remote.groupcomm.client.BatchSpcSender;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.TPartScheduler;
import org.elasql.schedule.tpart.bandit.BanditBasedRouter;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.graph.TxNode;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.server.task.Task;

/**
 * A collector that collects the features of transactions.
 *
 * @author Yu-Shan Lin
 */
public class SpCallPreprocessor extends Task {

	private final BanditTransactionDataCollector banditTransactionDataCollector;
	private final BanditTransactionContextFactory banditTransactionContextFactory;
	private final BanditModel banditModel;
	private final RoutingBanditActuator banditActuator;
	private BlockingQueue<StoredProcedureCall> spcQueue;
	private FeatureExtractor featureExtractor;

	// Components to simulate the scheduler
	private TPartStoredProcedureFactory factory;
	private BatchNodeInserter inserter;
	private TGraph graph;
	private boolean isBatching;
	private Estimator performanceEstimator;
	private HashSet<PrimaryKey> keyHasBeenRead = new HashSet<PrimaryKey>();

	// XXX: Cache last tx's routing destination
	private int lastTxRoutingDest = -1;

	// For collecting features
	private TransactionFeaturesRecorder featureRecorder;
	private TransactionDependencyRecorder dependencyRecorder;
	private TimeRelatedFeatureMgr timeRelatedFeatureMgr;

	public SpCallPreprocessor(TPartStoredProcedureFactory factory,
							  BatchNodeInserter inserter, TGraph graph,
							  boolean isBatching, TpartMetricWarehouse metricWarehouse,
							  Estimator performanceEstimator, BanditTransactionDataCollector banditTransactionDataCollector,
							  RoutingBanditActuator routingBanditActuator,
							  BanditTransactionContextFactory banditTransactionContextFactory,
							  RoutingBanditActuator banditActuator) {

		// For generating execution plan and sp task
		this.factory = factory;
		this.inserter = inserter;
		this.graph = graph;
		this.isBatching = isBatching;
		this.performanceEstimator = performanceEstimator;
		this.banditActuator = banditActuator;
		this.spcQueue = new LinkedBlockingQueue<StoredProcedureCall>();
		this.banditTransactionDataCollector = banditTransactionDataCollector;
		this.banditTransactionContextFactory = banditTransactionContextFactory;

		// For collecting features
		timeRelatedFeatureMgr = new TimeRelatedFeatureMgr();
		featureExtractor = new FeatureExtractor(metricWarehouse, timeRelatedFeatureMgr);
		if (TPartPerformanceManager.ENABLE_COLLECTING_DATA) {
			featureRecorder = new TransactionFeaturesRecorder();
			featureRecorder.startRecording();
			dependencyRecorder = new TransactionDependencyRecorder();
			dependencyRecorder.startRecording();
		}

		if (Elasql.SERVICE_TYPE == Elasql.ServiceType.HERMES_BANDIT) {
			BanditBasedRouter banditBasedRouter = (BanditBasedRouter) inserter;
			banditBasedRouter.setBanditTransactionDataCollector(banditTransactionDataCollector);
			banditBasedRouter.setBanditTransactionContextFactory(banditTransactionContextFactory);
		}
		if (Elasql.SERVICE_TYPE == Elasql.ServiceType.HERMES_BANDIT_SEQUENCER) {
			banditModel = new BanditModel(routingBanditActuator);
		} else {
			banditModel = null;
		}
	}

	public void preprocessSpCall(StoredProcedureCall spc) {
		if (!spc.isNoOpStoredProcCall())
			spcQueue.add(spc);
	}

	public void onTransactionCommit(long txNum, int masterId) {
		featureExtractor.onTransactionCommit(txNum);
		timeRelatedFeatureMgr.onTxCommit(masterId);
		banditTransactionContextFactory.removePartitionLoad(masterId);
	}

	@Override
	public void run() {
		List<TPartStoredProcedureTask> batchedTasks =
				new ArrayList<TPartStoredProcedureTask>();
		List<Serializable> sendingList = new ArrayList<Serializable>();

		Thread.currentThread().setName("sp-call-preprocessor");

		while (true) {
			try {
				// Take a SP call
				StoredProcedureCall spc = spcQueue.take();

				// Add to the sending list
				sendingList.add(spc);

				// Get the read-/write-set by creating a SP task
				TPartStoredProcedureTask task = createSpTask(spc);

				// Add normal SPs to the task batch
				if (task.getProcedureType() == ProcedureType.NORMAL ||
						task.getProcedureType() == ProcedureType.CONTROL ||
						task.getProcedureType() == ProcedureType.BANDIT) {
					// Pre-process the transaction 
					preprocess(spc, task);

					// Add to the schedule batch
					batchedTasks.add(task);
				}

				if (sendingList.size() >= BatchSpcSender.COMM_BATCH_SIZE) {
					// Send the SP call batch to total ordering
					Elasql.connectionMgr().sendTotalOrderRequest(sendingList);
					sendingList = new ArrayList<Serializable>();
				}

				// Process the batch as TPartScheduler does
				if (!isBatching || batchedTasks.size() >= TPartScheduler.SCHEDULE_BATCH_SIZE) {
					// Route the task batch
					routeBatch(batchedTasks);
					batchedTasks.clear();
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	private TPartStoredProcedureTask createSpTask(StoredProcedureCall spc) {
		if (!spc.isNoOpStoredProcCall()) {
			TPartStoredProcedure<?> sp = factory.getStoredProcedure(spc.getPid(), spc.getTxNum());
			sp.prepare(spc.getPars());
			return new TPartStoredProcedureTask(spc.getClientId(), spc.getConnectionId(),
					spc.getTxNum(), spc.getArrivedTime(), null, sp, null, null, -1);
		}

		return null;
	}

	private void preprocess(StoredProcedureCall spc, TPartStoredProcedureTask task) {
		if (task.getProcedureType() == ProcedureType.CONTROL){
			return;
		}
		if (task.getProcedureType() == ProcedureType.BANDIT) {
			if (Elasql.SERVICE_TYPE != Elasql.ServiceType.HERMES_BANDIT_SEQUENCER) {
				return;
			}
			banditModel.receiveReward(spc.getTxNum());
			return;
		}
		TransactionFeatures features = featureExtractor.extractFeatures(task, graph, keyHasBeenRead, lastTxRoutingDest);

		// records must be read from disk if they are never read.
		bookKeepKeys(task);

		// Record the feature if necessary
		if (TPartPerformanceManager.ENABLE_COLLECTING_DATA &&
				task.getProcedureType() != ProcedureType.CONTROL) {
			featureRecorder.record(features);
			dependencyRecorder.record(features);
		}

		// Estimate the performance if necessary
		if (performanceEstimator != null) {
			TransactionEstimation estimation = performanceEstimator.estimate(features);

			// Save the estimation
			spc.setMetadata(estimation.toBytes());
			task.setEstimation(estimation);
		// Bandit has no performanceEstimator
		} else if (Elasql.SERVICE_TYPE == Elasql.ServiceType.HERMES_BANDIT) {
			// Set transaction features that used in bandit
			BanditTransactionContext banditTransactionContext =
					banditTransactionContextFactory.buildContext(task.getTxNum(), features);

			banditTransactionDataCollector.addContext(banditTransactionContext);

			// TODO: metadata type
			spc.setMetadata(banditTransactionContext.getContext());
			task.setBanditTransactionContext(banditTransactionContext);
		} else if (Elasql.SERVICE_TYPE == Elasql.ServiceType.HERMES_BANDIT_SEQUENCER) {
			// Set transaction features that used in bandit
			BanditTransactionContext banditTransactionContext =
					banditTransactionContextFactory.buildContext(task.getTxNum(), features);

			int arm = banditModel.chooseArm(task.getTxNum(), banditTransactionContext.getContext());

			banditTransactionDataCollector.addContext(banditTransactionContext);
			banditTransactionDataCollector.addArm(new BanditTransactionArm(spc.getTxNum(), arm));

			banditTransactionContextFactory.addPartitionLoad(arm);

			double reward = calculateReward(features, arm);

			BanditTransactionReward banditTransactionReward = new BanditTransactionReward(task.getTxNum(), reward);
			banditActuator.addTransactionData(
					banditTransactionDataCollector.addRewardAndTakeOut(banditTransactionReward));

			// TODO: metadata type
			spc.setMetadata(arm);
			task.setAssignedPartition(arm);
		}
	}

	private void routeBatch(List<TPartStoredProcedureTask> batchedTasks) {
		// Insert the batch of tasks
		inserter.insertBatch(graph, batchedTasks);

		lastTxRoutingDest = timeRelatedFeatureMgr.pushInfo(graph);

		// Notify the estimator where the transactions are routed
		if (performanceEstimator != null) {
			for (TxNode node : graph.getTxNodes()) {
				performanceEstimator.notifyTransactionRoute(node.getTxNum(), node.getPartId());
			}
		}

		// add write back edges
		graph.addWriteBackEdge();

		// Clean up the tx nodes
		graph.clear();
	}

	private void bookKeepKeys(TPartStoredProcedureTask task) {
		for (PrimaryKey key : task.getReadSet()) {
			keyHasBeenRead.add(key);
		}
	}

	private double calculateReward(TransactionFeatures features, int arm) {
		Integer[] readDataDistributions = (Integer[]) features.getFeature("Remote Reads");
		Integer[] writeDataDistributions = (Integer[]) features.getFeature("Remote Writes");

		int readWriteCount = 0;
		for (int i = 0; i < readDataDistributions.length; i++) {
			readWriteCount += readDataDistributions[i] + writeDataDistributions[i];
		}

		double reward = 0.5;

		reward += 0.5 * ((double) (readDataDistributions[arm] + writeDataDistributions[arm]) / (double) readWriteCount);
		reward -= 0.5 * banditTransactionContextFactory.getPartitionLoad(arm);

		return reward;
	}
}
