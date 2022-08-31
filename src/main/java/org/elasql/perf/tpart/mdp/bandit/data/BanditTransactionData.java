package org.elasql.perf.tpart.mdp.bandit.data;

import org.apache.commons.math3.linear.ArrayRealVector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

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
		private volatile BanditTransactionContext banditTransactionContext;
		private volatile BanditTransactionReward banditTransactionReward;
		private volatile BanditTransactionArm banditTransactionArm;

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

		public List<String> missingComponents() {
			ArrayList<String> missingComponents = new ArrayList<>();
			if (banditTransactionContext == null) {
				missingComponents.add("banditTransactionContext");
			}
			if (banditTransactionArm == null) {
				missingComponents.add("banditTransactionArm");
			}
			if (banditTransactionReward == null) {
				missingComponents.add("banditTransactionReward");
			}
			return missingComponents;
		}

		public BanditTransactionData build() {
			List<String> missingComponents = missingComponents();
			if (!missingComponents.isEmpty()) {
				throw new RuntimeException(
						String.format("BanditTransactionData is not ready to build yet. %s are missing.",
								missingComponents));
			}
			if (banditTransactionContext.getTransactionNumber() != banditTransactionReward.getTransactionNumber() || banditTransactionReward.getTransactionNumber() != banditTransactionArm.getTransactionNumber()) {
				throw new RuntimeException("Cannot build BanditTransactionData: transaction number does not match");
			}

			return new BanditTransactionData(banditTransactionContext, banditTransactionReward, banditTransactionArm, banditTransactionArm.getTransactionNumber());
		}
	}
}
