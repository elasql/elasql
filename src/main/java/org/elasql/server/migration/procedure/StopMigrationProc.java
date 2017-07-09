package org.elasql.server.migration.procedure;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecuteProcedure;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class StopMigrationProc extends AllExecuteProcedure<StoredProcedureParamHelper> {

	public StopMigrationProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
		Elasql.migrationMgr().stopMigration();
		
		if(isSeqNode)
			Elasql.migrationMgr().onReceieveLaunchClayReq(null);
	}

	@Override
	protected void prepareKeys() {
		// Do nothing

	}
	
	@Override
	public boolean willResponseToClients(){
		return false;
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		System.out.println("Migration stops with tx number: " + txNum);

	}
}
