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
import org.elasql.storage.tx.concurrency.ConservativeOrderedCcMgr;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.tx.Transaction;

public class TwoPhaseBgPushProcedure extends CalvinStoredProcedure<TwoPhaseBgPushParamHelper> {
	private static Logger logger = Logger.getLogger(TwoPhaseBgPushProcedure.class.getName());

	private static final Constant FALSE = new IntegerConstant(0);
	private static final Constant TRUE = new IntegerConstant(1);

	private MgCrabMigrationMgr migraMgr = (MgCrabMigrationMgr) Elasql.migrationMgr();
	private int localNodeId = Elasql.serverId();
	
	private Set<PrimaryKey> pushingKeys = new HashSet<PrimaryKey>();
	private	Map<PrimaryKey, CachedRecord> lastPushedRecords;
	private Set<PrimaryKey> lastPushedKeys;

	public TwoPhaseBgPushProcedure(long txNum) {
		super(txNum, new TwoPhaseBgPushParamHelper());
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
	
	// Note: only the source and the destination node could execute this method
	protected void prepareKeys(ReadWriteSetAnalyzer analyzer) {
		lastPushedKeys = new HashSet<PrimaryKey>();

		// For phase One: The source node reads a set of records, then pushes to the
		// dest node.
		if (paramHelper.getCurrentPhase() == BgPushPhases.PHASE1 ||
				paramHelper.getCurrentPhase() == BgPushPhases.PIPELINING) {
			for (int i = 0; i < paramHelper.getPushingKeyCount(); i++) {
				PrimaryKey key = paramHelper.getPushingKey(i);
				// Important: We only push the un-migrated records to the dest
				if (!migraMgr.isMigrated(key))
					pushingKeys.add(key);
			}
			
			// Cache the push keys so that we won't have to broadcast these
			// keys again via total-ordering.
			if (localNodeId == paramHelper.getSourceNodeId())
				migraMgr.cachePushKeys(txNum, pushingKeys);
		}
		
		// For phase Two: The dest node acquires the locks, then storing them to the 
		// local storage.
		if (paramHelper.getCurrentPhase() == BgPushPhases.PHASE2 ||
				paramHelper.getCurrentPhase() == BgPushPhases.PIPELINING) {
			
			// If this is the first push, skip the second phase.
			if (paramHelper.getLastPushTxNum() == -1)
				return;
		
			// Retrieve the cached keys and records
			if (localNodeId == paramHelper.getSourceNodeId()) {
				lastPushedKeys = migraMgr.retrievePushKeys(paramHelper.getLastPushTxNum());
			} else if (localNodeId == paramHelper.getDestNodeId()) {
				lastPushedRecords = migraMgr.retrievePushedRecords(paramHelper.getLastPushTxNum());
				lastPushedKeys = lastPushedRecords.keySet();
			}
			
			// Ignore the migrated keys. We only insert the un-migrated records to the dest.
			Set<PrimaryKey> migratedKeys = new HashSet<PrimaryKey>();
			for (PrimaryKey key : lastPushedKeys) {
				if (migraMgr.isMigrated(key))
					migratedKeys.add(key);
			}
			
			lastPushedKeys.removeAll(migratedKeys);
		}
		
		// Note that we will do these phases in pipeline. The phase two of a BG will be
		// performed with the phase one of the next BG in the same time.
	}
	
	private ExecutionPlan generateExecutionPlan() {
		ExecutionPlan plan = new ExecutionPlan();
		
		// XXX: Should we lock the push keys on the source nodes?
		if (localNodeId == paramHelper.getDestNodeId()) {
			plan.setRemoteReadEnabled();
		}
		
		// Lock the pushed keys of 2nd phase to ensure Serializability
		if (paramHelper.getCurrentPhase() == BgPushPhases.PHASE2 ||
				paramHelper.getCurrentPhase() == BgPushPhases.PIPELINING) {
			if (localNodeId == paramHelper.getSourceNodeId()) {
				for (PrimaryKey key : lastPushedKeys) {
					plan.addLocalReadKey(key);
				}
			} else if (localNodeId == paramHelper.getDestNodeId()) {
				for (PrimaryKey key : lastPushedKeys) {
					plan.addLocalInsertKey(key);
				}
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
					lastPushedKeys.size() + " records at node." +
					paramHelper.getDestNodeId());
		
//		System.out.println("Push Keys: " + pushingKeys);
//		System.out.println("Store Keys: " + storingKeys);
		
		// The source node
		if (localNodeId == paramHelper.getSourceNodeId()) {
			if (paramHelper.getCurrentPhase() == BgPushPhases.PHASE1 ||
					paramHelper.getCurrentPhase() == BgPushPhases.PIPELINING) {
				// XXX: I'm not sure if we should do this
				// Quick fix: Release the locks immediately to prevent blocking the records in the source node
				Transaction tx = getTransaction();
				ConservativeOrderedCcMgr ccMgr = (ConservativeOrderedCcMgr) tx.concurrencyMgr();
				ccMgr.onTxCommit(tx);
				
				readAndPushInSource();
			}
		} else if (localNodeId == paramHelper.getDestNodeId()) {
			
			if (paramHelper.getCurrentPhase() == BgPushPhases.PHASE2 ||
					paramHelper.getCurrentPhase() == BgPushPhases.PIPELINING) {
				insertInDest(lastPushedRecords);
			}

			if (paramHelper.getCurrentPhase() == BgPushPhases.PHASE1 ||
					paramHelper.getCurrentPhase() == BgPushPhases.PIPELINING) {
				// TODO: Optimization: we can release locks at this point.
				lastPushedRecords = receiveInDest();
				if (lastPushedRecords != null && !lastPushedRecords.isEmpty())
					migraMgr.cachePushedRecords(txNum, lastPushedRecords);
			}
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("BG pushing tx." + txNum + " ends");
	}

	@Override
	protected void afterCommit() {
		if (localNodeId == paramHelper.getDestNodeId()) {
			if (paramHelper.getCurrentPhase() == BgPushPhases.PIPELINING) {
				if (pushingKeys.isEmpty())
					migraMgr.sendRangeFinishNotification();
				else
					migraMgr.scheduleNextTwoPhaseBGPushRequest(txNum, paramHelper.getCurrentPhase());
			} else if (paramHelper.getCurrentPhase() == BgPushPhases.PHASE1) {
				migraMgr.scheduleNextTwoPhaseBGPushRequest(txNum, BgPushPhases.PHASE2);
			} else {
				migraMgr.scheduleNextTwoPhaseBGPushRequest(txNum, BgPushPhases.PHASE1);
			}
		}
	}
	
	private void readAndPushInSource() {
		if (logger.isLoggable(Level.INFO))
			logger.info("BG pushing tx." + txNum + " reads " + pushingKeys.size()
					+ " records.");
		
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
			logger.info("BG pushing tx." + txNum + " pushes " + ts.size()
					+ " records to the dest. node. (Node." + paramHelper.getDestNodeId() + ")");

		// Push to the destination
		Elasql.connectionMgr().pushTupleSet(paramHelper.getDestNodeId(), ts);
		
	}
	
	private Map<PrimaryKey, CachedRecord> receiveInDest() {
		if (logger.isLoggable(Level.INFO))
			logger.info("BG pushing tx. " + txNum + " is receiving " + pushingKeys.size()
					+ " records from the source node. (Node." + paramHelper.getSourceNodeId() + ")");
		
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
			logger.info("BG pushing tx. " + txNum + " is storing " + lastPushedKeys.size()
					+ " records to the local storage.");

		// Store the cached records
		for (PrimaryKey key : lastPushedKeys) {
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
