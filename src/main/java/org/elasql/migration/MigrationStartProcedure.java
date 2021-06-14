package org.elasql.migration;

import java.util.Arrays;
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

public class MigrationStartProcedure extends CalvinStoredProcedure<StoredProcedureParamHelper> {

	private MigrationPlan migraPlan;
	private Object[] otherParams;
	
	public MigrationStartProcedure(long txNum) {
		super(txNum, StoredProcedureParamHelper.newDefaultParamHelper());
	}
	
	@Override
	protected ExecutionPlan analyzeParameters(Object[] pars) {
		// generate an empty execution plan
		ExecutionPlan plan = new ExecutionPlan();
		plan.setParticipantRole(ParticipantRole.ACTIVE);
		plan.setForceReadWriteTx(); // for stop-and-copy
		
		this.migraPlan = (MigrationPlan) pars[0];
		this.otherParams = Arrays.copyOfRange(pars, 1, pars.length);
		
		return plan;
	}

	@Override
	public void executeLogicInScheduler(Transaction tx) {
		Elasql.migrationMgr().initializeMigration(tx, migraPlan, otherParams);
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
