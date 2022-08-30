package org.elasql.perf.tpart.mdp.bandit.data;

import org.elasql.perf.MetricWarehouse;

import java.util.concurrent.ConcurrentHashMap;

public class BanditTransactionDataCollector implements MetricWarehouse {
	private final ConcurrentHashMap<Long, BanditTransactionData.Builder> map;

	public BanditTransactionDataCollector() {
		map = new ConcurrentHashMap<>();
	}

	/**
	 * Add reward for a transaction and take out built {@link BanditTransactionData}.
	 * This method assumes {@link BanditTransactionDataCollector#addContext} and
	 * {@link BanditTransactionDataCollector#addArm} is called before.
	 *
	 * @param banditTransactionReward reward for the transaction
	 * @return {@link BanditTransactionData}
	 */
	public BanditTransactionData addRewardAndTakeOut(BanditTransactionReward banditTransactionReward) {
		// preprocessor add features, so the builder must available here
		BanditTransactionData.Builder builder = map.remove(banditTransactionReward.getTransactionNumber());
		builder.addTransactionReward(banditTransactionReward);
		return builder.build();
	}

	public void addContext(BanditTransactionContext banditTransactionContext) {
		map.computeIfAbsent(banditTransactionContext.getTransactionNumber(), txNum -> new BanditTransactionData.Builder()).addBanditTransactionContext(banditTransactionContext);
	}

	public void addArm(BanditTransactionArm banditTransactionArm) {
		map.get(banditTransactionArm.getTransactionNumber()).addTransactionArm(banditTransactionArm);
	}
}
