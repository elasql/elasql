package org.elasql.perf.tpart.bandit.data;

import org.elasql.perf.TransactionMetricReport;

public class BanditTransactionReward implements TransactionMetricReport {
	private final long txNum;
	private final double reward;

	public BanditTransactionReward(long txNum, double reward) {
		this.txNum = txNum;
		this.reward = reward;
	}

	public double getReward() {
		return reward;
	}

	public long getTransactionNumber() {
		return txNum;
	}
}
