package org.elasql.schedule.tpart.bandit;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;
import org.elasql.perf.tpart.bandit.BanditRewardUpdateParamHelper;
import org.elasql.perf.tpart.bandit.BanditRewardUpdateProcedure;
import org.elasql.perf.tpart.bandit.data.BanditTransactionArm;
import org.elasql.perf.tpart.bandit.data.BanditTransactionDataCollector;
import org.elasql.procedure.tpart.TPartStoredProcedureTask;
import org.elasql.schedule.tpart.BatchNodeInserter;
import org.elasql.schedule.tpart.graph.TGraph;
import org.elasql.schedule.tpart.hermes.HermesNodeInserter;
import org.elasql.server.Elasql;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

import static org.elasql.perf.tpart.bandit.data.BanditTransactionContext.NUMBER_OF_CONTEXT;

public class BanditBasedRouter implements BatchNodeInserter {

	private static final UcbType LIN_UCB_TYPE;
	private static final double ALPHA;
	private static final OperationMode OPERATION_MODE;
	private static final long BOOTSTRAPPING_COUNT;
	private static final Logger logger = Logger.getLogger(BanditBasedRouter.class.getName());

	static {
		LIN_UCB_TYPE = UcbType.values()[ElasqlProperties.getLoader().getPropertyAsInteger(
				BanditBasedRouter.class.getName() + ".LIN_UCB_TYPE", 0)];
		ALPHA = ElasqlProperties.getLoader().getPropertyAsDouble(
				BanditBasedRouter.class.getName() + ".ALPHA", 5.0);
		OPERATION_MODE = OperationMode.values()[ElasqlProperties.getLoader().getPropertyAsInteger(
				BanditBasedRouter.class.getName() + ".OPERATION_MODE", 0)];
		BOOTSTRAPPING_COUNT = ElasqlProperties.getLoader().getPropertyAsLong(
				BanditBasedRouter.class.getName() + ".BOOTSTRAPPING_COUNT", 200000);
	}

	private LinUCB model;
	private BanditTransactionDataCollector banditTransactionDataCollector;
	private BatchNodeInserter boostrapInserter;
	private final BanditModelUpdater banditModelUpdater;

	public BanditBasedRouter() {
		switch (LIN_UCB_TYPE) {
			case LIN_UCB:
				model = new LinUCB(NUMBER_OF_CONTEXT, PartitionMetaMgr.NUM_PARTITIONS, ALPHA);
				break;
			case HYBRID_LIN_UCB:
				// use the same set of context for shared and non-shared features
				model = new HybridLinUCB(NUMBER_OF_CONTEXT, NUMBER_OF_CONTEXT, PartitionMetaMgr.NUM_PARTITIONS, ALPHA);
				break;
			default:
				throw new RuntimeException("Unknown model type " + LIN_UCB_TYPE);
		}
		switch (OPERATION_MODE) {
			case RL:
				boostrapInserter = null;
				break;
			case BOOTSTRAPPING:
				boostrapInserter = new HermesNodeInserter();
				break;
			default:
				throw new RuntimeException("Unknown operation mode" + OPERATION_MODE);
		}
		banditModelUpdater = new BanditModelUpdater();
		Elasql.taskMgr().runTask(banditModelUpdater);
	}

	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			LinUCB updatedModel = banditModelUpdater.getUpdatedModel(task.getTxNum());
			if (updatedModel != null) {
				model = updatedModel;
			}
			if (task.getProcedure().getClass().equals(BanditRewardUpdateProcedure.class)) {
				BanditRewardUpdateProcedure procedure = (BanditRewardUpdateProcedure) task.getProcedure();
				receiveReward(procedure.getParamHelper(), task.getTxNum());
			} else {
				int arm = insert(graph, task);
				if (banditTransactionDataCollector != null) {
					banditTransactionDataCollector.addArm(new BanditTransactionArm(task.getTxNum(), arm));
				}
			}
		}
	}

	public void setBanditTransactionDataCollector(BanditTransactionDataCollector banditTransactionDataCollector) {
		this.banditTransactionDataCollector = banditTransactionDataCollector;
	}

	private void receiveReward(BanditRewardUpdateParamHelper paramHelper, long transactionNumber) {
		long startTime = System.nanoTime();

		RealVector[] context = paramHelper.getContext();
		LinUCB copiedModel;
		if (LIN_UCB_TYPE == UcbType.HYBRID_LIN_UCB) {
			context = Arrays.stream(context).map(c -> c.append(c)).toArray(RealVector[]::new);
			copiedModel = new HybridLinUCB((HybridLinUCB) model);
		} else {
			copiedModel = new LinUCB(model);
		}

		banditModelUpdater.receiveRewards(copiedModel, transactionNumber,
				context, paramHelper.getArm(), paramHelper.getReward());

		// Debug
		logger.info(String.format("Receive rewards for %d transactions. Takes %f µs. Sample: %s",
				context.length, (System.nanoTime() - startTime) / 1000., paramHelper.getReward()[0]));
	}

	private int insert(TGraph graph, TPartStoredProcedureTask task) {
		long insertionStartTime = System.nanoTime();
		ArrayRealVector context = task.getBanditTransactionContext().getContext();
		if (LIN_UCB_TYPE == UcbType.HYBRID_LIN_UCB) {
			context = context.append(context);
		}

		int arm;
		String status;
		if (boostrapInserter != null) {
			boostrapInserter.insertBatch(graph, Collections.singletonList(task));
			arm = graph.getLastInsertedTxNode().getPartId();
			if (task.getTxNum() > BOOTSTRAPPING_COUNT) {
				boostrapInserter = null;
			}
			status = "Bootstrapping";
		} else {
			arm = model.chooseArm(context);
			graph.insertTxNode(task, arm);
			status = "RL";
		}

		logger.info(String.format("Status: %s. Choose arm %d for transaction %d. Takes %f µs. Context: %s",
				status, arm, task.getTxNum(), (System.nanoTime() - insertionStartTime) / 1000., context));

		return arm;
	}

	enum UcbType {
		LIN_UCB,
		HYBRID_LIN_UCB
	}

	enum OperationMode {
		RL,
		BOOTSTRAPPING,
	}
}


