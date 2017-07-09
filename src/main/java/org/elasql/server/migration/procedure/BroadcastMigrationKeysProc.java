package org.elasql.server.migration.procedure;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.server.Elasql;
import org.elasql.server.migration.MigrationManager;
import org.elasql.sql.RecordKey;

public class BroadcastMigrationKeysProc extends CalvinStoredProcedure<BroadcastMigrationKeysParamHelper> {

	public BroadcastMigrationKeysProc(long txNum) {
		super(txNum, new BroadcastMigrationKeysParamHelper());
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void prepareKeys() {
		System.out.println("I am " + this.localNodeId + "Source is " + paramHelper.getSouceNode() + " Dest is "
				+ paramHelper.getDestNode());
		Elasql.migrationMgr().addMigrationRanges(paramHelper.getMigrateKeys());

		if (isSeqNode) {
			System.out.println("I am " + this.localNodeId + "I commit BroadCastMigration");
			Elasql.migrationMgr().onReceiveAnalysisReq(null);
		}

	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean willResponseToClients() {
		return false;
	}

}
