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
import org.vanilladb.core.storage.tx.Transaction;

public class MigrationStartProcedure extends CalvinStoredProcedure<StoredProcedureParamHelper> {

	private Object[] params;
	
	public MigrationStartProcedure(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
	}
	
	@Override
	protected ExecutionPlan analyzeParameters(Object[] pars) {
		// generate an empty execution plan
		ExecutionPlan plan = new ExecutionPlan();
		plan.setParticipantRole(ParticipantRole.ACTIVE);
		plan.setForceReadWriteTx(); // for stop-and-copy
		
		params = pars;
		
		return plan;
	}

	@Override
	public void executeLogicInScheduler(Transaction tx) {
		Elasql.migrationMgr().initializeMigration(tx, params);
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
