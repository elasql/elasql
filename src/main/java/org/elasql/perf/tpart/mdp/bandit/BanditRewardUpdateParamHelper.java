package org.elasql.perf.tpart.mdp.bandit;

import org.apache.commons.math3.linear.ArrayRealVector;
import org.elasql.perf.tpart.mdp.bandit.data.BanditTransactionData;
import org.vanilladb.core.sql.Schema;
import org.vanilladb.core.sql.storedprocedure.SpResultRecord;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

/**
 * A helper that carries the latest parameters controlled by {@code RoutingBanditActuator}.
 * 
 * @author Yi-Sia Gao
 */
public class BanditRewardUpdateParamHelper extends StoredProcedureParamHelper {
	
	private ArrayRealVector[] context;
	private int[] arm;
	private double[] reward;

	@Override
	public void prepareParameters(Object... pars) {
		context = new ArrayRealVector[pars.length];
		arm = new int[pars.length];
		reward = new double[pars.length];

		for (int i = 0; i < pars.length; i++) {
			BanditTransactionData banditTransactionData = (BanditTransactionData) pars[i];
			context[i] = banditTransactionData.getContext();
			arm[i] = banditTransactionData.getArm();
			reward[i] = banditTransactionData.getReward();
		}
	}

	public ArrayRealVector[] getContext() {
		return context;
	}

	public int[] getArm() {
		return arm;
	}

	public double[] getReward() {
		return reward;
	}

	@Override
	public Schema getResultSetSchema() {
		return new Schema();
	}

	@Override
	public SpResultRecord newResultSetRecord() {
		return new SpResultRecord();
	}
}
