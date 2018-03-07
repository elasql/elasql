package org.elasql.server.migration.procedure;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecuteProcedure;
import org.elasql.server.Elasql;
import org.elasql.server.migration.MigrationManager;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class StopMigrationProc extends AllExecuteProcedure<StoredProcedureParamHelper> {

	public StopMigrationProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
		Elasql.migrationMgr().stopMigration();

		if (isSeqNode && MigrationManager.clayEpoch < MigrationManager.TOTAL_CLAY_EPOCH) {
			VanillaDb.taskMgr().runTask(new Task() {

				@Override
				public void run() {
					//
					if (!Elasql.migrationMgr().isReachTarget) {
						// Continue form last data trace
						Elasql.migrationMgr().updateMigratedVertexKeys();
						Elasql.migrationMgr().generateMigrationPlan();
					} else {
						//New mointoring
						Elasql.migrationMgr().cleanUpClay();
						Elasql.migrationMgr().onReceieveLaunchClayReq(null);
					}

				}
			});

		}
	}

	@Override
	protected void prepareKeys() {
		// Do nothing

	}

	@Override
	public boolean willResponseToClients() {
		return false;
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		System.out.println("Migration stops with tx number: " + txNum);

	}
}
