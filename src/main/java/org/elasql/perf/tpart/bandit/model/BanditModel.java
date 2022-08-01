package org.elasql.perf.tpart.bandit.model;

import org.apache.commons.math3.linear.RealVector;
import org.elasql.perf.tpart.bandit.RoutingBanditActuator;
import org.elasql.perf.tpart.bandit.model.linucb.HybridLinUCB;
import org.elasql.perf.tpart.bandit.model.linucb.LinUCB;
import org.elasql.schedule.tpart.bandit.BanditBasedRouter;
import org.elasql.server.Elasql;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;

import java.util.Arrays;
import java.util.logging.Logger;

import static org.elasql.perf.tpart.bandit.data.BanditTransactionContext.NUMBER_OF_CONTEXT;

public class BanditModel {
	private static final BanditBasedRouter.UcbType LIN_UCB_TYPE;
	private static final double ALPHA;
	private static final BanditBasedRouter.OperationMode OPERATION_MODE;
	private static final Logger logger = Logger.getLogger(BanditModel.class.getName());

	static {
		// TODO: borrow properties from BanditBasedRouter
		LIN_UCB_TYPE = BanditBasedRouter.UcbType.values()[ElasqlProperties.getLoader().getPropertyAsInteger(
				BanditBasedRouter.class.getName() + ".LIN_UCB_TYPE", 0)];
		ALPHA = ElasqlProperties.getLoader().getPropertyAsDouble(
				BanditBasedRouter.class.getName() + ".ALPHA", 5.0);
		OPERATION_MODE = BanditBasedRouter.OperationMode.values()[ElasqlProperties.getLoader().getPropertyAsInteger(
				BanditBasedRouter.class.getName() + ".OPERATION_MODE", 0)];
	}

	private final BanditModelUpdater banditModelUpdater;
	private final RoutingBanditActuator routingBanditActuator;
	private LinUCB model;

	public BanditModel(RoutingBanditActuator routingBanditActuator) {
		if (routingBanditActuator == null) {
			throw new NullPointerException(RoutingBanditActuator.class.getName() + " can not be null");
		}
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
				break;
			case BOOTSTRAPPING:
				throw new RuntimeException(BanditModel.class.getName() + " does not support bootstrap mode");
			default:
				throw new RuntimeException("Unknown operation mode" + OPERATION_MODE);
		}
		this.routingBanditActuator = routingBanditActuator;
		this.banditModelUpdater = new BanditModelUpdater();
		Elasql.taskMgr().runTask(banditModelUpdater);
	}

	public int chooseArm(long transactionNumber, RealVector context) {
		LinUCB updatedModel = banditModelUpdater.getUpdatedModel(transactionNumber);
		if (updatedModel != null) {
			model = updatedModel;
		}
		if (LIN_UCB_TYPE == BanditBasedRouter.UcbType.HYBRID_LIN_UCB) {
			context = context.append(context);
		}

		return model.chooseArm(context);
	}

	public void receiveReward(long transactionNumber) {
		long startTime = System.nanoTime();

		RoutingBanditActuator.ModelUpdateData modelUpdateData = routingBanditActuator.getModelUpdateData();
		if (modelUpdateData == null) {
			throw new RuntimeException("Model update data is not ready");
		}
		RealVector[] context = modelUpdateData.getContext();
		int[] arm = modelUpdateData.getArm();
		double[] reward = modelUpdateData.getReward();

		LinUCB copiedModel;
		if (LIN_UCB_TYPE == BanditBasedRouter.UcbType.HYBRID_LIN_UCB) {
			context = Arrays.stream(context).map(c -> c.append(c)).toArray(RealVector[]::new);
			copiedModel = new HybridLinUCB((HybridLinUCB) model);
		} else {
			copiedModel = new LinUCB(model);
		}

		banditModelUpdater.receiveRewards(copiedModel, transactionNumber, context, arm, reward);

		// Debug
		logger.info(String.format("Receive rewards for %d transactions. Takes %f µs. Sample: %s",
				context.length, (System.nanoTime() - startTime) / 1000., reward[0]));
	}
}
