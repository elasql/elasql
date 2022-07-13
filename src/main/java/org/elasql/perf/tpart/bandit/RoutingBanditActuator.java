package org.elasql.perf.tpart.bandit;

import org.elasql.perf.tpart.bandit.data.BanditTransactionData;
import org.elasql.perf.tpart.workload.BanditTransactionDataRecorder;
import org.elasql.schedule.tpart.bandit.BanditBasedRouter;
import org.elasql.server.Elasql;
import org.elasql.util.ElasqlProperties;
import org.vanilladb.core.server.task.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The actuator controls the rewards of {@code BanditBasedRouter}.
 *
 * @author Yi-Sia Gao
 */
public class RoutingBanditActuator extends Task {
	private static final int MAX_BATCH_SIZE;
	private static final int POLL_TIME_OUT;

	static {
		MAX_BATCH_SIZE = ElasqlProperties.getLoader().getPropertyAsInteger(
				BanditBasedRouter.class.getName() + ".MAX_BATCH_SIZE", 1000);
		POLL_TIME_OUT = ElasqlProperties.getLoader().getPropertyAsInteger(
				BanditBasedRouter.class.getName() + ".POLL_TIME_OUT", 1000);
	}

	private static final Logger logger = Logger.getLogger(RoutingBanditActuator.class.getName());

	private final BlockingQueue<BanditTransactionData> queue = new LinkedBlockingQueue<>();

	private final BanditTransactionDataRecorder banditTransactionDataRecorder = new BanditTransactionDataRecorder();

	public RoutingBanditActuator() {}

	public void addTransactionData(BanditTransactionData banditTransactionData) {
		queue.add(banditTransactionData);
	}
	
	@Override
	public void run() {
		Thread.currentThread().setName("routing-bandit-actuator");
		
		waitForServersReady();

		banditTransactionDataRecorder.startRecording();

		if (logger.isLoggable(Level.INFO))
			logger.info("Starting the routing bandit actuator");

		ArrayList<BanditTransactionData> pendingList = new ArrayList<>();

		while (true) {
			try {
				BanditTransactionData banditTransactionData = queue.poll(POLL_TIME_OUT, TimeUnit.MILLISECONDS);

				if (banditTransactionData != null) {
					pendingList.add(banditTransactionData);
					banditTransactionDataRecorder.record(banditTransactionData);

					if (pendingList.size() >= MAX_BATCH_SIZE) {
						// Issue an update transaction
						issueRewardUpdateTransaction(pendingList);
						pendingList.clear();
					}
				} else {
					if (pendingList.size() > 0) {
						issueRewardUpdateTransaction(pendingList);
						pendingList.clear();
					}
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void waitForServersReady() {
		while (!Elasql.connectionMgr().areAllServersReady()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	private void issueRewardUpdateTransaction(List<BanditTransactionData> banditTransactionDataList) {
//		ArrayList<BanditTransactionData> normalizedBanditTransactionDataList = new ArrayList<>(banditTransactionDataList.size());
//
//		double minReward = banditTransactionDataList.stream().mapToDouble(BanditTransactionData::getReward).min().orElse(Double.NaN);
//		double maxReward = banditTransactionDataList.stream().mapToDouble(BanditTransactionData::getReward).max().orElse(Double.NaN);
//		if (Double.isNaN(minReward) || Double.isNaN(maxReward)) {
//			throw new RuntimeException("Min or max of rewards is NaN");
//		}
//		double rewardRange = maxReward != minReward ? maxReward - minReward : maxReward;
//
//		for (BanditTransactionData banditTransactionData : banditTransactionDataList) {
//			BanditTransactionData.Builder builder = new BanditTransactionData.Builder();
//			long txNum = banditTransactionData.getTransactionNumber();
//			double reward = (banditTransactionData.getReward() - minReward) / rewardRange;
//			builder.addTransactionArm(new BanditTransactionArm(txNum, banditTransactionData.getArm()));
//			builder.addBanditTransactionContext(new BanditTransactionContext(txNum, banditTransactionData.getContext()));
//			builder.addTransactionReward(new BanditTransactionReward(txNum, reward));
//			normalizedBanditTransactionDataList.add(builder.build());
//		}
//
//
//		Object[] params = normalizedBanditTransactionDataList.toArray();
		Object[] params = banditTransactionDataList.toArray();
		// Send a store procedure call
		Elasql.connectionMgr().sendStoredProcedureCall(false,
				BanditStoredProcedureFactory.SP_BANDIT_RECEIVE_REWARDS, params);
	}
}
