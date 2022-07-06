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

import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

public class BanditBasedRouter implements BatchNodeInserter {
	private static final Logger logger = Logger.getLogger(BanditBasedRouter.class.getName());
	private final LinUCB model;
	private BanditTransactionDataCollector banditTransactionDataCollector;

	public BanditBasedRouter() {
		// TODO: Number of features hard coded
//		model = new LinUCB(PartitionMetaMgr.NUM_PARTITIONS * 3, PartitionMetaMgr.NUM_PARTITIONS, 1);
		model = new HybridLinUCB(PartitionMetaMgr.NUM_PARTITIONS * 2, PartitionMetaMgr.NUM_PARTITIONS * 2, PartitionMetaMgr.NUM_PARTITIONS, 10);
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
		model.receiveRewards(Arrays.stream(context).map(c -> c.append(c)).toArray(RealVector[]::new), paramHelper.getArm(), paramHelper.getReward());
//		model.receiveRewards(context, paramHelper.getArm(), paramHelper.getReward());

		// Debug
		logger.info(String.format("Receive rewards for %d transactions. Takes %f µs. Sample: %s", context.length, (System.nanoTime() - startTime) / 1000. ,paramHelper.getReward()[0]));
	}

	private int insert(TGraph graph, TPartStoredProcedureTask task) {
		long startTime = System.nanoTime();
		ArrayRealVector context = task.getBanditTransactionContext().getContext();
		int arm = model.chooseArm(context.append(context));
		graph.insertTxNode(task, arm);

		// Debug
		logger.info(String.format("Choose arm %d for transaction %d. Takes %f µs. Context: %s", arm, task.getTxNum(), (System.nanoTime() - startTime) / 1000., context));

		return arm;
	}
}


