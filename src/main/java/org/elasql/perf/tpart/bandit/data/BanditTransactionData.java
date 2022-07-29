package org.elasql.perf.tpart.bandit.data;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.apache.commons.math3.linear.RealVector;

import java.io.Serializable;

public class BanditTransactionData implements Serializable {
	private final ArrayRealVector context;
	private final int arm;
	private final double reward;
	private final long txNum;

	private BanditTransactionData(BanditTransactionContext banditTransactionContext, BanditTransactionReward banditTransactionReward, BanditTransactionArm banditTransactionArm, long txNum) {
		this.context = banditTransactionContext.getContext();
		this.arm = banditTransactionArm.getArm();
		this.reward = banditTransactionReward.getReward();
		this.txNum = txNum;
	}

	public ArrayRealVector getContext() {
		return context;
	}

	public double getReward() {
		return reward;
	}

	public int getArm() {
		return arm;
	}

	public long getTransactionNumber() {
		return txNum;
	}

	public static class Builder {
		private BanditTransactionContext banditTransactionContext;
		private BanditTransactionReward banditTransactionReward;
		private BanditTransactionArm banditTransactionArm;

		public Builder() {

		}

		public void addBanditTransactionContext(BanditTransactionContext banditTransactionContext) {
			this.banditTransactionContext = banditTransactionContext;
		}

		public void addTransactionReward(BanditTransactionReward banditTransactionReward) {
			this.banditTransactionReward = banditTransactionReward;
		}

		public void addTransactionArm(BanditTransactionArm banditTransactionArm) {
			this.banditTransactionArm = banditTransactionArm;
		}

		public boolean isReadyToBuild() {
			return banditTransactionContext != null && banditTransactionReward != null && banditTransactionArm != null;
		}

		public BanditTransactionData build() {
			if (!isReadyToBuild()) {
				throw new RuntimeException("BanditTransactionData is not ready to build yet");
			}
			if (banditTransactionContext.getTransactionNumber() != banditTransactionReward.getTransactionNumber() || banditTransactionReward.getTransactionNumber() != banditTransactionArm.getTransactionNumber()) {
				throw new RuntimeException("Cannot build BanditTransactionData: transaction number does not match");
			}

			return new BanditTransactionData(banditTransactionContext, banditTransactionReward, banditTransactionArm, banditTransactionArm.getTransactionNumber());
		}
	}
}
