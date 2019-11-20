package org.elasql.migration.tpart.sp;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;

public class MigrationStartProcedure extends TPartStoredProcedure<MigrationStartParamHelper> {

	public MigrationStartProcedure(long txNum) {
		super(txNum, new MigrationStartParamHelper());
	}

	@Override
	protected void prepareKeys() {
		Elasql.migrationMgr().initializeMigration(paramHelper.getMigrationPlan());
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		
	}

	@Override
	public double getWeight() {
		return 0;
	}
	
	@Override
	public ProcedureType getProcedureType() {
		// A utility procedure will be scheduled without batching
		return ProcedureType.UTILITY;
	}

}
