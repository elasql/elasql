package org.elasql.migration;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.schedule.calvin.ExecutionPlan;
import org.elasql.schedule.calvin.ExecutionPlan.ParticipantRole;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;
import org.vanilladb.core.storage.tx.Transaction;

public class MigrationEndProcedure extends CalvinStoredProcedure<StoredProcedureParamHelper> {

	private Object[] params;

	public MigrationEndProcedure(long txNum) {
		super(txNum, StoredProcedureParamHelper.newDefaultParamHelper());
	}
	
	@Override
	protected ExecutionPlan analyzeParameters(Object[] pars) {
		// generate an empty execution plan
		ExecutionPlan plan = new ExecutionPlan();
		plan.setParticipantRole(ParticipantRole.PASSIVE);
		plan.setForceReadWriteTx(); // for stop-and-copy
		
		params = pars;
		
		return plan;
	}
	
	@Override
	public void executeLogicInScheduler(Transaction tx) {
		Elasql.migrationMgr().finishMigration(tx, params);
	}
	
	@Override
	protected void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		
	}

	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		
	}

	@Override
	public boolean willResponseToClients() {
		return false;
	}

}
