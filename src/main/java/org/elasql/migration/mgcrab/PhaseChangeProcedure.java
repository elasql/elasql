package org.elasql.migration.mgcrab;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.schedule.calvin.ExecutionPlan;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.schedule.calvin.ExecutionPlan.ParticipantRole;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.storage.tx.Transaction;

public class PhaseChangeProcedure extends CalvinStoredProcedure<PhaseChangeParamHelper> {

	public PhaseChangeProcedure(long txNum) {
		super(txNum, new PhaseChangeParamHelper());
	}
	
	@Override
	protected ExecutionPlan analyzeParameters(Object[] pars) {
		// prepare parameters
		paramHelper.prepareParameters(pars);
		
		// generate an empty execution plan
		ExecutionPlan plan = new ExecutionPlan();
		plan.setParticipantRole(ParticipantRole.ACTIVE);
		return plan;
	}
	
	@Override
	protected void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		
	}
	
	@Override
	public void executeLogicInScheduler(Transaction tx) {
		MgCrabMigrationMgr migraMgr = (MgCrabMigrationMgr) Elasql.migrationMgr();
		migraMgr.changePhase(paramHelper.getNextPhase());
	}

	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		
	}

	@Override
	public boolean willResponseToClients() {
		return false;
	}
}
