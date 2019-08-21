package org.elasql.migration.squall;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.VanillaCoreCrud;
import org.elasql.procedure.calvin.CalvinStoredProcedure;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.schedule.calvin.ExecutionPlan;
import org.elasql.schedule.calvin.ExecutionPlan.ParticipantRole;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

public class BgPushProcedure extends CalvinStoredProcedure<BgPushParamHelper> {
	private static Logger logger = Logger.getLogger(BgPushProcedure.class.getName());

	private static final Constant FALSE = new IntegerConstant(0);
	private static final Constant TRUE = new IntegerConstant(1);

	private SquallMigrationMgr migraMgr = (SquallMigrationMgr) Elasql.migrationMgr();
	private int localNodeId = Elasql.serverId();
	
	private Set<RecordKey> pushingKeys = new HashSet<RecordKey>();

	public BgPushProcedure(long txNum) {
		super(txNum, new BgPushParamHelper());
	}
	
	@Override
	protected ExecutionPlan analyzeParameters(Object[] pars) {
		ExecutionPlan plan;
		
		// prepare parameters
		paramHelper.prepareParameters(pars);

		// Due to the high cost of pre-process,
		// we only require the source and the destination node to prepare the keys.
		if (localNodeId == paramHelper.getSourceNodeId() ||
				localNodeId == paramHelper.getDestNodeId()) {
			
			// analyze read-write set
			prepareKeys(null);
			
			// generate an execution plan for locking storing keys
			plan = generateExecutionPlan();
		} else {
			// create an empty plan for other nodes
			plan = new ExecutionPlan();
		}
		
		// update migration range
		if (paramHelper.getMigrationRangeUpdate() != null)
			migraMgr.updateMigrationRange(paramHelper.getMigrationRangeUpdate());
		
		return plan;
	}

	@Override
	public void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		for (int i = 0; i < paramHelper.getPushingKeyCount(); i++) {
			RecordKey key = paramHelper.getPushingKey(i);
			if (!migraMgr.isMigrated(key))
				pushingKeys.add(key);
		}
	}
	
	@Override
	public boolean willResponseToClients() {
		return false;
	}
	
	private ExecutionPlan generateExecutionPlan() {
		ExecutionPlan plan = new ExecutionPlan();
		
		if (localNodeId == paramHelper.getSourceNodeId()) {
			for (RecordKey key : pushingKeys) {
				plan.setRemoteReadEnabled();
				plan.addLocalReadKey(key);
			}
		} else if (localNodeId == paramHelper.getDestNodeId()) {
			for (RecordKey key : pushingKeys) {
				plan.setRemoteReadEnabled();
				plan.addLocalInsertKey(key);
			}
		}
		
		if (localNodeId == paramHelper.getSourceNodeId())
			plan.setParticipantRole(ParticipantRole.PASSIVE);
		else if (localNodeId == paramHelper.getDestNodeId())
			plan.setParticipantRole(ParticipantRole.ACTIVE);
		
		return plan;
	}

	@Override
	protected void executeTransactionLogic() {
		long start = System.nanoTime();
		
		if (localNodeId == paramHelper.getSourceNodeId()) {
			if (logger.isLoggable(Level.INFO))
				logger.info("BG pushing tx." + txNum + " will pushes " +
						pushingKeys.size() + " records from node." +
						paramHelper.getSourceNodeId() + " to node." +
						paramHelper.getDestNodeId() + ".");
			
			readAndPushInSource();
		} else if (localNodeId == paramHelper.getDestNodeId()) {
			Map<RecordKey, CachedRecord> readCache = receiveInDest();
			insertInDest(readCache);
		}
		
		long time = System.nanoTime() - start;
		double timeMs = time / 1000_000.0;
		
		if (logger.isLoggable(Level.INFO))
			logger.info("BG pushing tx." + txNum + " ends (it takes " + timeMs + " ms to finish)");
	}

	@Override
	protected void afterCommit() {
		if (localNodeId == paramHelper.getDestNodeId()) {
			migraMgr.scheduleNextBGPushRequest();
		}
	}
	
	private void readAndPushInSource() {
		// Wait for the pull request
		Set<Integer> dests = new HashSet<Integer>();
		dests.add(paramHelper.getDestNodeId());
		waitForMigrationPullRequests(dests);
		
		// Construct pushing tuple set
		TupleSet ts = new TupleSet(-1);

		// Construct key sets
		Map<String, Set<RecordKey>> keysPerTables = new HashMap<String, Set<RecordKey>>();
		for (RecordKey key : pushingKeys) {
			Set<RecordKey> keys = keysPerTables.get(key.getTableName());
			if (keys == null) {
				keys = new HashSet<RecordKey>();
				keysPerTables.put(key.getTableName(), keys);
			}
			keys.add(key);
		}

		// Batch read the records per table
		for (Map.Entry<String, Set<RecordKey>> entry : keysPerTables.entrySet()) {
			Map<RecordKey, CachedRecord> recordMap = VanillaCoreCrud.batchRead(entry.getValue(), getTransaction());

			for (RecordKey key : entry.getValue()) {
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
			logger.info("BG pushing tx. " + txNum + " pushes " + ts.size()
					+ " records to the dest. node. (Node." + paramHelper.getDestNodeId() + ")");

		// Push to the destination
		Elasql.connectionMgr().pushTupleSet(paramHelper.getDestNodeId(), ts);
		
	}
	
	private Map<RecordKey, CachedRecord> receiveInDest() {
		if (logger.isLoggable(Level.INFO))
			logger.info("BG pushing tx. " + txNum + " is receiving " + pushingKeys.size()
					+ " records from the source node. (Node." + paramHelper.getSourceNodeId() + ")");

		// Send a pull request
		Set<Integer> sources = new HashSet<Integer>();
		sources.add(paramHelper.getSourceNodeId());
		sendMigrationPullRequests(sources);
		
		Map<RecordKey, CachedRecord> recordMap = new HashMap<RecordKey, CachedRecord>();

		// Receive the data from the source node and save them
		for (RecordKey k : pushingKeys) {
			CachedRecord rec = cacheMgr.readFromRemote(k);
			recordMap.put(k, rec);
		}
		
		return recordMap;
	}
	
	private void insertInDest(Map<RecordKey, CachedRecord> cachedRecords) {
		if (logger.isLoggable(Level.INFO))
			logger.info("BG pushing tx. " + txNum + " is storing " + pushingKeys.size()
					+ " records to the local storage.");

		// Store the cached records
		for (RecordKey key : pushingKeys) {
			CachedRecord rec = cachedRecords.get(key);
			
			if (rec == null)
				throw new RuntimeException("Something wrong: " + key);

			// Flush them to the local storage engine
			if (rec.getVal("exists").equals(TRUE)) {
				rec.getFldValMap().remove("exists");
				rec.getDirtyFldNames().remove("exists");
				
				cacheMgr.insert(key, rec.getFldValMap());
			}
		}
	}

	@Override
	protected void executeSql(Map<RecordKey, CachedRecord> readings) {
		// do nothing
	}
}

