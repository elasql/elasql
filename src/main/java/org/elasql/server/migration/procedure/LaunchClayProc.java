package org.elasql.server.migration.procedure;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.AllExecuteProcedure;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.storedprocedure.StoredProcedureParamHelper;

public class LaunchClayProc extends AllExecuteProcedure<StoredProcedureParamHelper> {
	public LaunchClayProc(long txNum) {
		super(txNum, StoredProcedureParamHelper.DefaultParamHelper());
		if (isSeqNode)
			Elasql.migrationMgr().startMonitoring();
		System.out.println("Clay Monitoring starts with tx number: " + txNum);
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
		

	}
}
