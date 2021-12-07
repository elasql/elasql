package org.elasql.perf.tpart.control;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.sql.PrimaryKey;

public class ControlParamUpdateProcedure extends TPartStoredProcedure<ControlParamUpdateParamHelper> {

	public ControlParamUpdateProcedure(long txNum) {
		super(txNum, new ControlParamUpdateParamHelper());
	}

	@Override
	public ControlParamUpdateParamHelper getParamHelper() {
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
		return ProcedureType.CONTROL;
	}
}
