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
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.util.ElasqlProperties;

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import static org.elasql.perf.tpart.bandit.data.BanditTransactionContext.NUMBER_OF_CONTEXT;

public class BanditBasedRouter implements BatchNodeInserter {

	enum UcbType {
		LIN_UCB,
		HYBRID_LIN_UCB
	}

	private static final UcbType LIN_UCB_TYPE;
	private static final double ALPHA;

	static {
		LIN_UCB_TYPE = UcbType.values()[ElasqlProperties.getLoader().getPropertyAsInteger(
				BanditBasedRouter.class.getName() + ".LIN_UCB_TYPE", 0)];
		ALPHA = ElasqlProperties.getLoader().getPropertyAsDouble(
				BanditBasedRouter.class.getName() + ".ALPHA", 5.0);
	}

	private static final Logger logger = Logger.getLogger(BanditBasedRouter.class.getName());
	private final LinUCB model;
	private BanditTransactionDataCollector banditTransactionDataCollector;

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
	}

	@Override
	public void insertBatch(TGraph graph, List<TPartStoredProcedureTask> tasks) {
		for (TPartStoredProcedureTask task : tasks) {
			if (task.getProcedure().getClass().equals(BanditRewardUpdateProcedure.class)) {
				BanditRewardUpdateProcedure procedure = (BanditRewardUpdateProcedure) task.getProcedure();
				receiveReward(procedure.getParamHelper());
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

	private void receiveReward(BanditRewardUpdateParamHelper paramHelper) {
		long startTime = System.nanoTime();
		RealVector[] context = paramHelper.getContext();
		if (LIN_UCB_TYPE == UcbType.HYBRID_LIN_UCB) {
			context = Arrays.stream(context).map(c -> c.append(c)).toArray(RealVector[]::new);
		}
		model.receiveRewards(context, paramHelper.getArm(), paramHelper.getReward());

		// Debug
		logger.info(String.format("Receive rewards for %d transactions. Takes %f µs. Sample: %s", context.length, (System.nanoTime() - startTime) / 1000. ,paramHelper.getReward()[0]));
	}

	private int insert(TGraph graph, TPartStoredProcedureTask task) {
		long startTime = System.nanoTime();
		ArrayRealVector context = task.getBanditTransactionContext().getContext();
		if (LIN_UCB_TYPE == UcbType.HYBRID_LIN_UCB) {
			context = context.append(context);
		}
		int arm = model.chooseArm(context);
		graph.insertTxNode(task, arm);

		// Debug
		logger.info(String.format("Choose arm %d for transaction %d. Takes %f µs. Context: %s", arm, task.getTxNum(), (System.nanoTime() - startTime) / 1000., context));

		return arm;
	}
}


