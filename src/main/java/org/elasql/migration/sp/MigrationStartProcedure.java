package org.elasql.migration.sp;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;

public class MigrationStartProcedure extends CalvinStoredProcedure<MigrationStartParamHelper> {

	public MigrationStartProcedure(long txNum) {
		super(txNum, new MigrationStartParamHelper());
	}
	
	@Override
	protected void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		Elasql.migrationMgr().initializeMigration(
				paramHelper.getNewPartitionPlan(), paramHelper.getInitialPhase());
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		
	}
	
	public boolean willResponseToClients() {
		return false;
	}
}
