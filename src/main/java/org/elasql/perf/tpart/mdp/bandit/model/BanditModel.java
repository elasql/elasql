package org.elasql.perf.tpart.mdp.bandit.model;

import static org.elasql.perf.tpart.mdp.bandit.data.BanditTransactionContext.NUMBER_OF_CONTEXT;

import java.util.logging.Logger;

import org.apache.commons.math3.linear.RealVector;
import org.elasql.perf.tpart.mdp.bandit.RoutingBanditActuator;
import org.elasql.perf.tpart.mdp.bandit.model.linucb.HybridLinUCB;
import org.elasql.perf.tpart.mdp.bandit.model.linucb.LinUCB;
import org.elasql.server.Elasql;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;

public class BanditModel {
	public static enum UcbType {
		LIN_UCB, HYBRID_LIN_UCB
	}

	public static enum OperationMode {
		RL, BOOTSTRAPPING,
	}

	private static final UcbType LIN_UCB_TYPE;
	private static final double ALPHA;
	private static final OperationMode OPERATION_MODE;
	private static final Logger logger = Logger.getLogger(BanditModel.class.getName());

	static {
		LIN_UCB_TYPE = UcbType.values()[ElasqlProperties.getLoader().getPropertyAsInteger(
				BanditModel.class.getName() + ".LIN_UCB_TYPE", 0)];
		ALPHA = ElasqlProperties.getLoader().getPropertyAsDouble(
				BanditModel.class.getName() + ".ALPHA", 5.0);
		OPERATION_MODE = OperationMode.values()[ElasqlProperties.getLoader().getPropertyAsInteger(
				BanditModel.class.getName() + ".OPERATION_MODE", 0)];
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
		if (LIN_UCB_TYPE == UcbType.HYBRID_LIN_UCB) {
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
		if (LIN_UCB_TYPE == UcbType.HYBRID_LIN_UCB) {
			copiedModel = new HybridLinUCB((HybridLinUCB) model);
		} else {
			copiedModel = new LinUCB(model);
		}

		banditModelUpdater.receiveRewards(copiedModel, transactionNumber, context, arm, reward);

		// Debug
		logger.info(String.format("Receive rewards for %d transactions. Takes %f µs. Sample: %s", context.length,
				(System.nanoTime() - startTime) / 1000., reward[0]));
	}
}
