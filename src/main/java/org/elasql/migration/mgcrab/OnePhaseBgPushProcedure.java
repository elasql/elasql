package org.elasql.migration.mgcrab;

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
import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;

public class OnePhaseBgPushProcedure extends CalvinStoredProcedure<OnePhaseBgPushParamHelper> {
	private static Logger logger = Logger.getLogger(OnePhaseBgPushProcedure.class.getName());

	private static final Constant FALSE = new IntegerConstant(0);
	private static final Constant TRUE = new IntegerConstant(1);

	private MgCrabMigrationMgr migraMgr = (MgCrabMigrationMgr) Elasql.migrationMgr();
	private int localNodeId = Elasql.serverId();
	
	private Set<PrimaryKey> pushingKeys = new HashSet<PrimaryKey>();

	public OnePhaseBgPushProcedure(long txNum) {
		super(txNum, new OnePhaseBgPushParamHelper());
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
	public boolean willResponseToClients() {
		return false;
	}
	
	protected void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		for (int i = 0; i < paramHelper.getPushingKeyCount(); i++) {
			PrimaryKey key = paramHelper.getPushingKey(i);
			// Important: We only push the un-migrated records to the dest
			if (!migraMgr.isMigrated(key))
				pushingKeys.add(key);
		}
	}
	
	private ExecutionPlan generateExecutionPlan() {
		ExecutionPlan plan = new ExecutionPlan();
		
		if (localNodeId == paramHelper.getSourceNodeId()) {
			for (PrimaryKey key : pushingKeys) {
				plan.setRemoteReadEnabled();
				plan.addLocalReadKey(key);
			}
		} else if (localNodeId == paramHelper.getDestNodeId()) {
			for (PrimaryKey key : pushingKeys) {
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
			Map<PrimaryKey, CachedRecord> readCache = receiveInDest();
			insertInDest(readCache);
		}
		
		long time = System.nanoTime() - start;
		double timeMs = time / 1000_000.0;
		
		if (logger.isLoggable(Level.INFO))
			logger.info("BG pushing tx." + txNum + " ends (it takes " + timeMs + " ms to finish)");
	}

	@Override
	protected void afterCommit() {
		if (localNodeId == paramHelper.getDestNodeId())
			migraMgr.scheduleNextOnePhaseBGPushRequest();
	}
	
	private void readAndPushInSource() {
		// Wait for the pull request
		Set<Integer> dests = new HashSet<Integer>();
		dests.add(paramHelper.getDestNodeId());
		waitForMigrationPullRequests(dests);
		
		// Construct pushing tuple set
		TupleSet ts = new TupleSet(-1);

		// Construct key sets
		Map<String, Set<PrimaryKey>> keysPerTables = new HashMap<String, Set<PrimaryKey>>();
		for (PrimaryKey key : pushingKeys) {
			Set<PrimaryKey> keys = keysPerTables.get(key.getTableName());
			if (keys == null) {
				keys = new HashSet<PrimaryKey>();
				keysPerTables.put(key.getTableName(), keys);
			}
			keys.add(key);
		}

		// Batch read the records per table
		for (Map.Entry<String, Set<PrimaryKey>> entry : keysPerTables.entrySet()) {
			Map<PrimaryKey, CachedRecord> recordMap = VanillaCoreCrud.batchRead(entry.getValue(), getTransaction());

			for (PrimaryKey key : entry.getValue()) {
				// System.out.println(key);
				CachedRecord rec = recordMap.get(key);

				// Prevent null pointer exceptions in the destination node
				if (rec == null) {
					rec = new CachedRecord(key);
					rec.setSrcTxNum(txNum);
					rec.addFldVal("exists", FALSE);
				} else
					rec.addFldVal("exists", TRUE);

				ts.addTuple(key, txNum, txNum, rec);
			}
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("BG pushing tx. " + txNum + " pushes " + ts.size()
					+ " records to the dest. node. (Node." + paramHelper.getDestNodeId() + ")");

		// Push to the destination
		Elasql.connectionMgr().pushTupleSet(paramHelper.getDestNodeId(), ts);
		
	}
	
	private Map<PrimaryKey, CachedRecord> receiveInDest() {
		if (logger.isLoggable(Level.INFO))
			logger.info("BG pushing tx. " + txNum + " is receiving " + pushingKeys.size()
					+ " records from the source node. (Node." + paramHelper.getSourceNodeId() + ")");

		// Send a pull request
		Set<Integer> sources = new HashSet<Integer>();
		sources.add(paramHelper.getSourceNodeId());
		sendMigrationPullRequests(sources);
		
		Map<PrimaryKey, CachedRecord> recordMap = new HashMap<PrimaryKey, CachedRecord>();

		// Receive the data from the source node and save them
		for (PrimaryKey k : pushingKeys) {
			CachedRecord rec = cacheMgr.readFromRemote(k);
			recordMap.put(k, rec);
		}
		
		return recordMap;
	}
	
	private void insertInDest(Map<PrimaryKey, CachedRecord> cachedRecords) {
		if (logger.isLoggable(Level.INFO))
			logger.info("BG pushing tx. " + txNum + " is storing " + pushingKeys.size()
					+ " records to the local storage.");

		// Store the cached records
		for (PrimaryKey key : pushingKeys) {
			CachedRecord rec = cachedRecords.get(key);
			
			if (rec == null)
				throw new RuntimeException("Something wrong: " + key);

			// Flush them to the local storage engine
			if (rec.getVal("exists").equals(TRUE)) {
				rec.removeField("exists");
				cacheMgr.insert(key, rec);
			}
		}
	}

	@Override
	protected void executeSql(Map<PrimaryKey, CachedRecord> readings) {
		// do nothing
	}
}
