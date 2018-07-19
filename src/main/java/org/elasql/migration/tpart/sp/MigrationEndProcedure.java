package org.elasql.migration.tpart.sp;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.tpart.TPartStoredProcedure;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class MigrationEndProcedure extends TPartStoredProcedure<StoredProcedureParamHelper> {

	public MigrationEndProcedure(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
		Elasql.migrationMgr().finishMigration();
	}

	@Override
	protected void prepareKeys() {
		
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
