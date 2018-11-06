package org.elasql.migration;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.schedule.calvin.ExecutionPlan;
import org.elasql.schedule.calvin.ExecutionPlan.ParticipantRole;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class MigrationEndProcedure extends CalvinStoredProcedure<StoredProcedureParamHelper> {

	private Object[] params;

	public MigrationEndProcedure(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
	}
	
	@Override
	public void prepare(Object... pars) {
		// generate an empty execution plan
		execPlan = new ExecutionPlan();
		execPlan.setParticipantRole(ParticipantRole.PASSIVE);
		execPlan.setForceReadWriteTx();
		
		params = pars;
	}
	
	@Override
	public void executeLogicInScheduler() {
		Elasql.migrationMgr().finishMigration(tx, params);
	}
	
	@Override
	protected void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		
	}

	@Override
	public boolean willResponseToClients() {
		return false;
	}

}
