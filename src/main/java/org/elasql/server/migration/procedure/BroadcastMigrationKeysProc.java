package org.elasql.server.migration.procedure;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;

public class BroadcastMigrationKeysProc extends CalvinStoredProcedure<BroadcastMigrationKeysParamHelper>{

	public BroadcastMigrationKeysProc(long txNum) {
		super(txNum, new BroadcastMigrationKeysParamHelper());
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void prepareKeys() {
		Elasql.migrationMgr().addMigrationRanges(paramHelper.getMigrateKeys());
		
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		// TODO Auto-generated method stub
		
	}
	
	@Override
	public boolean willResponseToClients(){
		return false;
	}

}
