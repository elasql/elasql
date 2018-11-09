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
import org.elasql.sql.RecordKey;
import org.elasql.storage.tx.concurrency.ConservativeOrderedCcMgr;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.tx.Transaction;

public class TwoPhaseBgPushProcedure extends CalvinStoredProcedure<TwoPhaseBgPushParamHelper> {
	private static Logger logger = Logger.getLogger(TwoPhaseBgPushProcedure.class.getName());

	private static Constant FALSE = new IntegerConstant(0);
	private static Constant TRUE = new IntegerConstant(1);

	// XXX: This does not work when there are two concurrent job with the same destination.
	private static Map<RecordKey, CachedRecord> pushingCacheInDest;

	private MgCrabMigrationMgr migraMgr = (MgCrabMigrationMgr) Elasql.migrationMgr();
	private int localNodeId = Elasql.serverId();
	
	private Set<RecordKey> pushingKeys = new HashSet<RecordKey>();
	private Set<RecordKey> storingKeys = new HashSet<RecordKey>();

	public TwoPhaseBgPushProcedure(long txNum) {
		super(txNum, new TwoPhaseBgPushParamHelper());
	}

	@Override
	protected ExecutionPlan analyzeParameters(Object[] pars) {
		// prepare parameters
		paramHelper.prepareParameters(pars);

		// analyze read-write set
		prepareKeys(null);
		
		// generate an execution plan for locking storing keys
		ExecutionPlan plan = generateExecutionPlan();
		
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
		// For phase One: The source node reads a set of records, then pushes to the
		// dest node.
		for (RecordKey key : paramHelper.getPushingKeys())
			// Important: We only push the un-migrated records in the dest
			if (!migraMgr.isMigrated(key))
				pushingKeys.add(key);
		
		// For phase Two: The dest node acquire the locks, then storing them to the 
		// local storage.
		for (RecordKey key : paramHelper.getStoringKeys())
			// Important: We only insert the un-migrated records in the dest
			if (!migraMgr.isMigrated(key))
				storingKeys.add(key);
		
		// Note that we will do this in pipeline. The phase two of a BG will be
		// performed with the phase one of the next BG in the same time.
	}
	
	private ExecutionPlan generateExecutionPlan() {
		ExecutionPlan plan = new ExecutionPlan();
		
		// XXX: Should we lock the push keys on the source nodes?
		if (localNodeId == paramHelper.getDestNodeId()) {
			plan.setRemoteReadEnabled();
		}
		
		if (localNodeId == paramHelper.getSourceNodeId()) {
			for (RecordKey key : storingKeys) {
				plan.addLocalReadKey(key);
			}
		} else if (localNodeId == paramHelper.getDestNodeId()) {
			for (RecordKey key : storingKeys) {
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
		if (logger.isLoggable(Level.INFO))
			logger.info("BG pushing tx." + txNum + " will pushes " +
					pushingKeys.size() + " records from node." +
					paramHelper.getSourceNodeId() + " to node." +
					paramHelper.getDestNodeId() + " and stores " +
					storingKeys.size() + " records at node." +
					paramHelper.getDestNodeId());
		
//		System.out.println("Push Keys: " + pushingKeys);
//		System.out.println("Store Keys: " + storingKeys);
		
		// The source node
		if (localNodeId == paramHelper.getSourceNodeId()) {
			// XXX: I'm not sure if we should do this
			// Quick fix: Release the locks immediately to prevent blocking the records in the source node
			Transaction tx = getTransaction();
			ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();
			ccMgr.onTxCommit(tx);
			
			readAndPushInSource();
		} else if (localNodeId == paramHelper.getDestNodeId()) {
			insertInDest(pushingCacheInDest);
			pushingCacheInDest = receiveInDest();
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("BG pushing tx." + txNum + " ends");
	}

	@Override
	protected void afterCommit() {
		if (localNodeId == paramHelper.getDestNodeId()) {
			migraMgr.scheduleNextBGPushRequest();
		}
	}
	
	private void readAndPushInSource() {
		if (logger.isLoggable(Level.INFO))
			logger.info("BG pushing tx." + txNum + " reads " + pushingKeys.size()
					+ " records.");
		
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
			logger.info("BG pushing tx." + txNum + " pushes " + ts.size()
					+ " records to the dest. node. (Node." + paramHelper.getDestNodeId() + ")");

		// Push to the destination
		Elasql.connectionMgr().pushTupleSet(paramHelper.getDestNodeId(), ts);
		
	}
	
	private Map<RecordKey, CachedRecord> receiveInDest() {
		if (logger.isLoggable(Level.INFO))
			logger.info("BG pushing tx. " + txNum + " is receiving " + pushingKeys.size()
					+ " records from the source node. (Node." + paramHelper.getSourceNodeId() + ")");
		
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
			logger.info("BG pushing tx. " + txNum + " is storing " + storingKeys.size()
					+ " records to the local storage.");

		// Store the cached records
		for (RecordKey key : storingKeys) {
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
