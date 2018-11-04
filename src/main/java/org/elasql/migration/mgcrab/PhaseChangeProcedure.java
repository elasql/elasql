package org.elasql.migration.mgcrab;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;

public class PhaseChangeProcedure extends CalvinStoredProcedure<PhaseChangeParamHelper> {

	public PhaseChangeProcedure(long txNum) {
		super(txNum, new PhaseChangeParamHelper());
	}
	
	@Override
	protected void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		MgCrabMigrationMgr migraMgr = (MgCrabMigrationMgr) Elasql.migrationMgr();
		migraMgr.changePhase(paramHelper.getNextPhase());
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		
	}

	@Override
	public boolean willResponseToClients() {
		return false;
	}
}
