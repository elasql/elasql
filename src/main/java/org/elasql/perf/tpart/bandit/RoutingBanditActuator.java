package org.elasql.perf.tpart.bandit;

import org.elasql.perf.tpart.bandit.data.BanditTransactionData;
import org.elasql.server.Elasql;
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
	private static final Logger logger = Logger.getLogger(RoutingBanditActuator.class.getName());

	private final BlockingQueue<BanditTransactionData> queue = new LinkedBlockingQueue<>();

	public RoutingBanditActuator() {}

	public void addTransactionData(BanditTransactionData banditTransactionData) {
		queue.add(banditTransactionData);
	}
	
	@Override
	public void run() {
		Thread.currentThread().setName("routing-bandit-actuator");
		
		waitForServersReady();
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Starting the routing bandit actuator");

		ArrayList<BanditTransactionData> pendingList = new ArrayList<>();

		while (true) {
			try {
				BanditTransactionData banditTransactionData = queue.poll(3000, TimeUnit.MILLISECONDS);

				if (banditTransactionData != null) {
					pendingList.add(banditTransactionData);
					if (pendingList.size() >= 100) {
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
		Object[] params = banditTransactionDataList.toArray();
		// Send a store procedure call
		Elasql.connectionMgr().sendStoredProcedureCall(false, 
				BanditStoredProcedureFactory.SP_BANDIT_RECEIVE_REWARDS, params);
	}
}
