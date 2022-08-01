package org.elasql.perf.tpart.bandit.model;

import org.apache.commons.math3.linear.RealVector;
import org.apache.commons.math3.util.Pair;
import org.elasql.perf.tpart.bandit.model.linucb.HybridLinUCB;
import org.elasql.perf.tpart.bandit.model.linucb.LinUCB;
import org.vanilladb.core.server.task.Task;

import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

public class BanditModelUpdater extends Task {
	private static final Logger logger = Logger.getLogger(BanditModelUpdater.class.getName());
	private final BlockingQueue<ModelUpdate> pendingModelUpdates = new ArrayBlockingQueue<>(100);
	private final BlockingQueue<Pair<Long, LinUCB>> updatedModels = new ArrayBlockingQueue<>(100);
	private final Queue<Long> updateModelTransactionNumbers = new ArrayDeque<>();

	public void receiveRewards(LinUCB model, long transactionNumber, RealVector[] context, int[] arm, double[] reward) {
		long updateTransactionNumber = transactionNumber + context.length / 2;
		ModelUpdate modelUpdate = new ModelUpdate(model, updateTransactionNumber, context, arm, reward);
		updateModelTransactionNumbers.offer(updateTransactionNumber);
		try {
			pendingModelUpdates.put(modelUpdate);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	public LinUCB getUpdatedModel(long transactionNumber) {
		Long updateModelTransactionNumber = updateModelTransactionNumbers.peek();
		if (updateModelTransactionNumber == null || updateModelTransactionNumber != transactionNumber) {
			return null;
		}

		try {
			Pair<Long, LinUCB> updatedModel = updatedModels.take();
			if (updatedModel.getFirst() != transactionNumber) {
				throw new RuntimeException("Wrong update transaction number " + transactionNumber);
			}
			return updatedModel.getSecond();
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void run() {
		Thread.currentThread().setName("routing-bandit-model-updater");

		logger.info("Starting the routing bandit model updater");

		while (true) {
			try {
				ModelUpdate modelUpdate = pendingModelUpdates.take();
				RealVector[] context = modelUpdate.context;

				if (modelUpdate.model.getClass().equals(HybridLinUCB.class)) {
					context = Arrays.stream(context).map(c -> c.append(c)).toArray(RealVector[]::new);
				}

				modelUpdate.model.receiveRewards(context, modelUpdate.arm, modelUpdate.reward);

				updatedModels.put(new Pair<>(modelUpdate.updateTransactionNumber, modelUpdate.model));
			} catch (InterruptedException e) {
				throw new RuntimeException(e);
			}
		}
	}

	private static class ModelUpdate {
		private final LinUCB model;
		private final long updateTransactionNumber;
		private final RealVector[] context;
		private final int[] arm;
		private final double[] reward;

		private ModelUpdate(LinUCB model, long updateTransactionNumber, RealVector[] context, int[] arm, double[] reward) {
			this.model = model;
			this.updateTransactionNumber = updateTransactionNumber;
			this.context = context;
			this.arm = arm;
			this.reward = reward;
		}
	}
}
