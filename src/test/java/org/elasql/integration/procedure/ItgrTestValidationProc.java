package org.elasql.integration.procedure;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.sql.PrimaryKey;

public class ItgrTestValidationProc extends TPartStoredProcedure<ItgrTestValidationProcParamHelper> {
	public ItgrTestValidationProc(long txNum) {
		super(txNum, new ItgrTestValidationProcParamHelper());
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
