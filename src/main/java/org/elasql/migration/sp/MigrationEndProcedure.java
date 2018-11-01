package org.elasql.migration.sp;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class MigrationEndProcedure extends CalvinStoredProcedure<StoredProcedureParamHelper> {

	public MigrationEndProcedure(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
	}
	
	@Override
	protected void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		Elasql.migrationMgr().finishMigration();
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		
	}

	@Override
	public boolean willResponseToClients() {
		return false;
	}

}
