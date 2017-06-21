package org.elasql.server.migration.procedure;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecuteProcedure;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class StartMigrationProc extends AllExecuteProcedure<StoredProcedureParamHelper> {
	
	public StartMigrationProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
		Elasql.migrationMgr().startMigration();
	}

	@Override
	protected void prepareKeys() {
		// Do nothing
		
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		System.out.println("Migration starts with tx number: " + txNum);
		
	}
}