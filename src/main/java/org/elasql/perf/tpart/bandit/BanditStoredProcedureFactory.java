package org.elasql.perf.tpart.bandit;

import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.procedure.tpart.TPartStoredProcedureFactory;

public class BanditStoredProcedureFactory implements TPartStoredProcedureFactory {

	public static final int SP_BANDIT_RECEIVE_REWARDS = -20220526;

	private final TPartStoredProcedureFactory underlayingFactory;
	
	public BanditStoredProcedureFactory(TPartStoredProcedureFactory underlayingFactory) {
		this.underlayingFactory = underlayingFactory;
	}
	
	@Override
	public TPartStoredProcedure<?> getStoredProcedure(int pid, long txNum) {
		TPartStoredProcedure<?> sp;
		if (pid == SP_BANDIT_RECEIVE_REWARDS) {
			sp = new BanditRewardUpdateProcedure(txNum);
		} else {
			sp = underlayingFactory.getStoredProcedure(pid, txNum);
		}
		return sp;
	}
}
