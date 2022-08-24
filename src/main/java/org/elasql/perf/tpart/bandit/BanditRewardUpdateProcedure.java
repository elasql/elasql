package org.elasql.perf.tpart.bandit;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.sql.PrimaryKey;

import java.util.Map;

public class BanditRewardUpdateProcedure extends TPartStoredProcedure<BanditRewardUpdateParamHelper> {

	public BanditRewardUpdateProcedure(long txNum) {
		super(txNum, new BanditRewardUpdateParamHelper());
	}

	@Override
	public BanditRewardUpdateParamHelper getParamHelper() {
		return paramHelper;
	}

	@Override
	public double getWeight() {
		return 0;
	}

	@Override
	protected void prepareKeys() {
		// Do nothing
	}

	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		// Do nothing
	}
	
	@Override
	public ProcedureType getProcedureType() {
		return ProcedureType.BANDIT;
	}
}
