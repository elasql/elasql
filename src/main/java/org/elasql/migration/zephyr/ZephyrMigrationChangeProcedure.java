package org.elasql.migration.zephyr;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.schedule.calvin.ExecutionPlan;
import org.elasql.schedule.calvin.ExecutionPlan.ParticipantRole;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.Transaction;

public class ZephyrMigrationChangeProcedure extends CalvinStoredProcedure<StoredProcedureParamHelper> {

	private Object[] params;

	public ZephyrMigrationChangeProcedure(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
	}
	
	@Override
	protected ExecutionPlan analyzeParameters(Object[] pars) {
		// generate an empty execution plan
		ExecutionPlan plan = new ExecutionPlan();
		plan.setParticipantRole(ParticipantRole.ACTIVE);
		
		return plan;
	}
	
	@Override
	public void executeLogicInScheduler(Transaction tx) {
		ZephyrMigrationMgr migraMgr = (ZephyrMigrationMgr) Elasql.migrationMgr();
		migraMgr.changePhase(tx, params);
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