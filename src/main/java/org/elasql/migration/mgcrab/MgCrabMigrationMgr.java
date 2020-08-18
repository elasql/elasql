package org.elasql.migration.mgcrab;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.migration.MigrationComponentFactory;
import org.elasql.migration.MigrationMgr;
import org.elasql.migration.MigrationPlan;
import org.elasql.migration.MigrationRange;
import org.elasql.migration.MigrationRangeFinishMessage;
import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.migration.MigrationSettings;
import org.elasql.migration.MigrationSystemController;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.schedule.calvin.CalvinScheduler;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.schedule.calvin.mgcrab.CaughtUpAnalyzer;
import org.elasql.schedule.calvin.mgcrab.CrabbingAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * The migration manager that exists in each node. Job: 
 * (1) trace the migration states, 
 * (2) initialize a background push transaction, and 
 * (3) send the finish notification to the main controller on the sequencer node.
 */
public class MgCrabMigrationMgr implements MigrationMgr {
	private static Logger logger = Logger.getLogger(MgCrabMigrationMgr.class.getName());
	
	private Phase currentPhase = Phase.NORMAL;
	private List<MigrationRange> migrationRanges;
	private List<MigrationRange> pushRanges = new ArrayList<MigrationRange>(); // the ranges whose destination is this node.
	private PartitionPlan newPartitionPlan;
	
	// XXX: We assume that the destination only has one range to receive at a time. 
	private MigrationRangeUpdate lastUpdate;
	
	// For the source node to record the last pushed keys in the two phase
	// background pushing.
	private Map<Long, Set<PrimaryKey>> txNumToPushKeys = 
			new ConcurrentHashMap<Long, Set<PrimaryKey>>();
	
	// Two phase background pushing stores the pushed records on the
	// destination node.
	private Map<Long, Map<PrimaryKey, CachedRecord>> txNumToPushedRecords = 
			new ConcurrentHashMap<Long, Map<PrimaryKey, CachedRecord>>();
	
	private MigrationComponentFactory comsFactory;
	
	public MgCrabMigrationMgr(MigrationComponentFactory comsFactory) {
		this.comsFactory = comsFactory;
	}
	
	public void initializeMigration(Transaction tx, MigrationPlan plan, Object[] params) {
		PartitionPlan newPartPlan = plan.getNewPart();
		Phase initialPhase = (Phase) params[0];
		
		if (logger.isLoggable(Level.INFO)) {
			long time = System.currentTimeMillis() - CalvinScheduler.FIRST_TX_ARRIVAL_TIME.get();
			PartitionPlan currentPartPlan = Elasql.partitionMetaMgr().getPartitionPlan();
			logger.info(String.format("a new migration starts at %d. Current Plan: %s, New Plan: %s"
					, time / 1000, currentPartPlan, newPartPlan));
		}
		
		// Initialize states
		currentPhase = initialPhase;
		migrationRanges = plan.getMigrationRanges(comsFactory);
		for (MigrationRange range : migrationRanges)
			if (range.getDestPartId() == Elasql.serverId())
				pushRanges.add(range);
		newPartitionPlan = newPartPlan;
		
		// Start background pushes
		if (!pushRanges.isEmpty()) {
			if (MgcrabSettings.BG_PUSH_START_DELAY == 0) {
				if (MgcrabSettings.ENABLE_TWO_PHASE_BG_PUSH) {
					if (MgcrabSettings.ENABLE_PIPELINING_TWO_PHASE_BG)
						scheduleNextTwoPhaseBGPushRequest(-1, BgPushPhases.PIPELINING);
					else
						scheduleNextTwoPhaseBGPushRequest(-1, BgPushPhases.PHASE1);
				} else {
					scheduleNextOnePhaseBGPushRequest();
				}
			} else {
				new Thread(new Runnable() {
					@Override
					public void run() {
						if (MgcrabSettings.BG_PUSH_START_DELAY > 0) {
							try {
								Thread.sleep(MgcrabSettings.BG_PUSH_START_DELAY);
							} catch (InterruptedException e) {
								e.printStackTrace();
							}
						}
						
						if (logger.isLoggable(Level.INFO)) {
							long time = System.currentTimeMillis() - CalvinScheduler.FIRST_TX_ARRIVAL_TIME.get();
							logger.info(String.format("the background pushes start at %d."
									, time / 1000));
						}
						
						if (MgcrabSettings.ENABLE_TWO_PHASE_BG_PUSH) {
							if (MgcrabSettings.ENABLE_PIPELINING_TWO_PHASE_BG)
								scheduleNextTwoPhaseBGPushRequest(-1, BgPushPhases.PIPELINING);
							else
								scheduleNextTwoPhaseBGPushRequest(-1, BgPushPhases.PHASE1);
						} else {
							scheduleNextOnePhaseBGPushRequest();
						}
					}
				}).start();
			}
		}
		
		if (logger.isLoggable(Level.INFO)) {
			logger.info(String.format("migration ranges: %s", migrationRanges.toString()));
		}
	}
	
	public void scheduleNextOnePhaseBGPushRequest() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				for (MigrationRange range : pushRanges) {
					Set<PrimaryKey> chunk = range.generateNextMigrationChunk(
							MigrationSettings.USE_BYTES_FOR_CHUNK_SIZE, MigrationSettings.CHUNK_SIZE);
					if (chunk.size() > 0) {
						sendOnePhaseBGPushRequest(range.generateStatusUpdate(), chunk, 
								range.getSourcePartId(), range.getDestPartId());
						return;
					}
				}
				
				// If it reach here, it means that there is no more chunk
				sendRangeFinishNotification();
			}
		}).start();
	}
	
	public void scheduleNextTwoPhaseBGPushRequest(long lastPushedTxNum, BgPushPhases phase) {
		new Thread(new Runnable() {
			@Override
			public void run() {
				// Phase 1? proceed to phase 2
				if (phase == BgPushPhases.PHASE2) {
					sendTwoPhaseBGPushRequest(BgPushPhases.PHASE2, new HashSet<PrimaryKey>(),
							lastUpdate.getSourcePartId(), lastUpdate.getDestPartId(), lastPushedTxNum, lastUpdate);
					return;
				}
				
				// XXX: If there is multiple ranges to this destinations
				// we should know which range pairs to this transaction number
				for (MigrationRange range : pushRanges) {
					Set<PrimaryKey> chunk = range.generateNextMigrationChunk(
							MigrationSettings.USE_BYTES_FOR_CHUNK_SIZE, MigrationSettings.CHUNK_SIZE);
					if (chunk.size() > 0) {
						if (phase == BgPushPhases.PIPELINING) {
							sendTwoPhaseBGPushRequest(BgPushPhases.PIPELINING, chunk, range.getSourcePartId(), 
									range.getDestPartId(), lastPushedTxNum, lastUpdate);
						} else {
							sendTwoPhaseBGPushRequest(BgPushPhases.PHASE1, chunk, range.getSourcePartId(), 
									range.getDestPartId(), lastPushedTxNum, null);
						}
						lastUpdate = range.generateStatusUpdate();
						return;
					}
				}
				
				// If it reach here, this should the last push
				if (phase == BgPushPhases.PIPELINING) {
					sendTwoPhaseBGPushRequest(BgPushPhases.PIPELINING, new HashSet<PrimaryKey>(), 
							lastUpdate.getSourcePartId(), lastUpdate.getDestPartId(), lastPushedTxNum, lastUpdate);
				} else {
					sendRangeFinishNotification();
				}
			}
		}).start();
	}
	
	public void sendOnePhaseBGPushRequest(MigrationRangeUpdate update, Set<PrimaryKey> chunk,
			int sourceNodeId, int destNodeId) {
		if (logger.isLoggable(Level.INFO))
			logger.info("send a one-phase background push request with " + chunk.size() + " keys.");
		
		// Prepare the parameters
		Object[] params = new Object[4 + chunk.size()];
		
		params[0] = update;
		params[1] = sourceNodeId;
		params[2] = destNodeId;
		params[3] = chunk.size();
		
		int i = 4;
		for (PrimaryKey key : chunk)
			params[i++] = key;
		
		// Send a store procedure call
		Elasql.connectionMgr().sendStoredProcedureCall(false, 
				MgCrabStoredProcFactory.SP_ONE_PHASE_BG_PUSH, params);
	}
	
	public void sendTwoPhaseBGPushRequest(BgPushPhases phase, Set<PrimaryKey> chunk, int sourceNodeId, 
			int destNodeId, long lastPushedTxNum, MigrationRangeUpdate update) {
		if (logger.isLoggable(Level.INFO))
			logger.info("send a two-phase background push request with " + chunk.size() + " keys. (" + phase + ")");
		
		// Prepare the parameters
		Object[] params = new Object[6 + chunk.size()];
		
		params[0] = phase;
		params[1] = update;
		params[2] = sourceNodeId;
		params[3] = destNodeId;
		params[4] = lastPushedTxNum;
		params[5] = chunk.size();
		
		int i = 6;
		for (PrimaryKey key : chunk)
			params[i++] = key;
		
		// Send a store procedure call
		Elasql.connectionMgr().sendStoredProcedureCall(false, 
				MgCrabStoredProcFactory.SP_TWO_PHASE_BG_PUSH, params);
	}
	
	public void cachePushKeys(long pushTxNum, Set<PrimaryKey> pushKeys) {
		txNumToPushKeys.put(pushTxNum, pushKeys);
	}
	
	public void cachePushedRecords(long pushTxNum, Map<PrimaryKey, CachedRecord> pushedRecords) {
		txNumToPushedRecords.put(pushTxNum, pushedRecords);
	}
	
	public Set<PrimaryKey> retrievePushKeys(long pushTxNum) {
		return txNumToPushKeys.remove(pushTxNum);
	}
	
	public Map<PrimaryKey, CachedRecord> retrievePushedRecords(long pushTxNum) {
		return txNumToPushedRecords.remove(pushTxNum);
	}
	
	public void changePhase(Phase newPhase) {
		if (logger.isLoggable(Level.INFO)) {
			long time = System.currentTimeMillis() - CalvinScheduler.FIRST_TX_ARRIVAL_TIME.get();
			logger.info(String.format("the migration changes to %s phase at %d ms."
					, newPhase, time / 1000));
		}
		
		currentPhase = newPhase;
	}
	
	public void updateMigrationRange(MigrationRangeUpdate update) {
		for (MigrationRange range : migrationRanges)
			if (range.updateMigrationStatus(update))
				return;
		throw new RuntimeException(String.format("This is no match for the update", update));
	}
	
	// XXX: Currently, we do not identify which range finished.
	// We only count how many ranges are finished, instead.
	public void sendRangeFinishNotification() {
		if (logger.isLoggable(Level.INFO))
			logger.info("send a range finish notification to the system controller");
		
		TupleSet ts = new TupleSet(MigrationSystemController.MSG_RANGE_FINISH);
		ts.setMetadata(new MigrationRangeFinishMessage(pushRanges.size())); // notify how many ranges are migrated
		Elasql.connectionMgr().pushTupleSet(MigrationSystemController.CONTROLLER_NODE_ID, ts);
	}
	
	public void finishMigration(Transaction tx, Object[] params) {
		if (logger.isLoggable(Level.INFO)) {
			long time = System.currentTimeMillis() - CalvinScheduler.FIRST_TX_ARRIVAL_TIME.get();
			logger.info(String.format("the migration finishes at %d."
					, time / 1000));
		}
		
		// Change the current partition plan of the system
		Elasql.partitionMetaMgr().setNewPartitionPlan(newPartitionPlan);
		
		// Clear the migration states
		currentPhase = Phase.NORMAL;
		lastUpdate = null;
		migrationRanges.clear();
		pushRanges.clear();
	}
	
	public boolean isMigratingRecord(PrimaryKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return true;
		return false;
	}
	
	public boolean isMigrated(PrimaryKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return range.isMigrated(key);
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public void setMigrated(PrimaryKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key)) {
				range.setMigrated(key);
				return;
			}
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public int checkSourceNode(PrimaryKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return range.getSourcePartId();
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public int checkDestNode(PrimaryKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return range.getDestPartId();
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public Phase getCurrentPhase() {
		return currentPhase;
	}
	
	public boolean isInMigration() {
		return currentPhase != Phase.NORMAL;
	}
	
	public ReadWriteSetAnalyzer newAnalyzer() {
		if (currentPhase == Phase.CRABBING)
			return new CrabbingAnalyzer();
		else if (currentPhase == Phase.CAUGHT_UP)
			return new CaughtUpAnalyzer();
		else
			throw new RuntimeException(
					String.format("We haven't implement %s phase yet.", currentPhase));
	}
}
