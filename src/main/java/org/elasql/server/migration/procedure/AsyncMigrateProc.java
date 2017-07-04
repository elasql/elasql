package org.elasql.server.migration.procedure;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.VanillaCoreCrud;
import org.elasql.cache.calvin.CalvinCacheMgr;
import org.elasql.cache.calvin.CalvinPostOffice;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.server.Elasql;
import org.elasql.server.migration.MigrationManager;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

public class AsyncMigrateProc extends CalvinStoredProcedure<AsyncMigrateParamHelper> {
	private static Logger logger = Logger.getLogger(AsyncMigrateProc.class.getName());

	private static Constant FALSE = new IntegerConstant(0);
	private static Constant TRUE = new IntegerConstant(1);

	public AsyncMigrateProc(long txNum) {
		super(txNum, new AsyncMigrateParamHelper());
	}

	@Override
	public void prepareKeys() {
		// Lock the pushing records
		for (RecordKey key : paramHelper.getPushingKeys())
			addWriteKey(key);
		isAsyncMigrateProc = true;
		
		// isBgPush = true;
	}

	@Override
	protected void executeTransactionLogic() {
		System.out.println("I'm "+Elasql.serverId() + "I receive Asunc Migration");
		if (Elasql.serverId() == Elasql.migrationMgr().getSourcePartition())
			executeSourceLogic();
		else if (Elasql.serverId() == Elasql.migrationMgr().getDestPartition())
			executeDestLogic();

		if (logger.isLoggable(Level.INFO))
			logger.info("Asnyc pushing tx. " + txNum + " ends");
	}

	@Override
	protected void afterCommit() {
		if (Elasql.serverId() == Elasql.migrationMgr().getDestPartition()) {
			TupleSet ts = new TupleSet(MigrationManager.SINK_ID_ASYNC_PUSHING);
			Elasql.connectionMgr().pushTupleSet(Elasql.migrationMgr().getSourcePartition(), ts);
		}
	}
	
	@Override
	public boolean willResponseToClients(){
		return false;
	}

	private void executeSourceLogic() {
		
		
		if (logger.isLoggable(Level.INFO))
			logger.info("Asnyc pushing tx. " + txNum + " starts in the source node");

		// For Squall: Pulling-based migration
		// Wait for a pull request
		waitForPullRequest();
		System.out.println("Get pull request!");

		// Construct pushing tuple set
		TupleSet ts = new TupleSet(-2);

		// Construct key sets
		Map<String, Set<RecordKey>> keysPerTables = new HashMap<String, Set<RecordKey>>();
		for (RecordKey key : paramHelper.getPushingKeys()) {
			Set<RecordKey> keys = keysPerTables.get(key.getTableName());
			if (keys == null) {
				keys = new HashSet<RecordKey>();
				keysPerTables.put(key.getTableName(), keys);
			}
			keys.add(key);
		}

		// Batch read the records per table
		for (Set<RecordKey> keys : keysPerTables.values()) {
			Map<RecordKey, CachedRecord> recordMap = VanillaCoreCrud.batchRead(keys, tx);

			for (RecordKey key : keys) {
				// System.out.println(key);
				CachedRecord rec = recordMap.get(key);

				// Prevent null pointer exceptions in the destination node
				if (rec == null) {
					rec = new CachedRecord();
					rec.setSrcTxNum(txNum);
					rec.setVal("exists", FALSE);
				} else
					rec.setVal("exists", TRUE);

				ts.addTuple(key, txNum, txNum, rec);
			}
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Asnyc pushing tx. " + txNum + " pushes " + ts.size()
					+ " records to the dest. node.\nFirst record: " + paramHelper.getPushingKeys()[0]);

		// Push to the destination
		Elasql.connectionMgr().pushTupleSet(Elasql.migrationMgr().getDestPartition(), ts);
	}

	private void executeDestLogic() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Asnyc pushing tx. " + txNum + " starts in the destination node");

		// For Squall: Pulling-based migration
		// Send a pull request
		sendAPullRequest(Elasql.migrationMgr().getSourcePartition());
		
		// Receive the data from the source node and save them
		for (RecordKey key : paramHelper.getPushingKeys()) {
			//System.out.println("++++++++++Receieve "+key);
			CachedRecord rec = cacheMgr.readFromRemote(key);

			// Flush them to the local storage engine
			if (rec.getVal("exists").equals(TRUE)) {
				rec.getFldValMap().remove("exists");
				rec.getDirtyFldNames().remove("exists");
				cacheMgr.insert(key, rec.getFldValMap());
			}
		}
		cacheMgr.flush();
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		// Do nothing

	}
}
