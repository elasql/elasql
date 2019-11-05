package org.elasql.server.migration.procedure;

import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;

public class BroadcastMigrationKeysProc extends CalvinStoredProcedure<BroadcastMigrationKeysParamHelper> {

	public BroadcastMigrationKeysProc(long txNum) {
		super(txNum, new BroadcastMigrationKeysParamHelper());
		// TODO Auto-generated constructor stub
	}

	@Override
	protected void prepareKeys() {
		Elasql.migrationMgr().addMigrationRepresentKeys(paramHelper.getMigrateKeys());
		Elasql.migrationMgr().setSourcePartition(paramHelper.getSouceNode());
		Elasql.migrationMgr().setDestPartition(paramHelper.getDestNode());
		
//		System.out.println("Broadcast I am " + this.localNodeId + "Source is " + Elasql.migrationMgr().getSourcePartition() + " Dest is "
//				+ Elasql.migrationMgr().getDestPartition());
		
		if (isSeqNode) {
//			System.out.println("I am " + this.localNodeId + "I commit BroadCastMigration");
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
