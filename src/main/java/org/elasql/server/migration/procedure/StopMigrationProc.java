package org.elasql.server.migration.procedure;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecuteProcedure;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class StopMigrationProc extends AllExecuteProcedure<StoredProcedureParamHelper> {
	private static Logger logger = Logger.getLogger(StopMigrationProc.class.getName());

	public StopMigrationProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
		if (logger.isLoggable(Level.INFO))
			logger.info("Migration stops with tx number: " + txNum);
		Elasql.migrationMgr().stopMigration();
	}

	@Override
	protected void prepareKeys() {
		// Do nothing
	}
	
	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		
	}
}
