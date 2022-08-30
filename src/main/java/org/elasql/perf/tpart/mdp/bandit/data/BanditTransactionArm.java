package org.elasql.perf.tpart.mdp.bandit.data;

public class BanditTransactionArm {
	private final long txNum;
	private final int arm;

	public BanditTransactionArm(long txNum, int arm) {
		this.txNum = txNum;
		this.arm = arm;
	}

	public int getArm() {
		return arm;
	}

	public long getTransactionNumber() {
		return txNum;
	}
}
