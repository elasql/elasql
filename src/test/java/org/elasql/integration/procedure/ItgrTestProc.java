package org.elasql.integration.procedure;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.sql.PrimaryKey;

public class ItgrTestProc extends TPartStoredProcedure<ItgrTestProcParamHelper> {
	// Protected resource
//	protected long txNum;
//	protected H paramHelper;
//	protected int localNodeId;
//	protected Transaction tx;

	public ItgrTestProc(long txNum) {
		super(txNum, new ItgrTestProcParamHelper());
	}

	@Override
	public double getWeight() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	protected void prepareKeys() {
		// TODO Auto-generated method stub

	}

	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		// TODO Auto-generated method stub

	}
}
