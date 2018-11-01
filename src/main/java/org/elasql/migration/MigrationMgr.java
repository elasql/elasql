package org.elasql.migration;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.sp.MigrationStoredProcFactory;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.schedule.calvin.migration.CrabbingAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionPlan;

/**
 * The migration manager that exists in each node. Job: 
 * (1) trace the migration states, 
 * (2) initialize a background push transaction, and 
 * (3) send the finish notification to the main controller on the sequencer node.
 */
public abstract class MigrationMgr {
	private static Logger logger = Logger.getLogger(MigrationMgr.class.getName());
	
	private static final int CHUNK_SIZE = 1_000_000; // 1MB
	
	private Phase currentPhase = Phase.NORMAL;
	private List<MigrationRange> migrationRanges;
	private List<MigrationRange> pushRanges = new ArrayList<MigrationRange>(); // the ranges whose destination is this node.
	private Set<RecordKey> lastChunk;
	private MigrationRangeUpdate lastUpdate;
	private PartitionPlan newPartitionPlan;
	
	public void initializeMigration(PartitionPlan newPartPlan, Phase initialPhase) {
		if (logger.isLoggable(Level.INFO)) {
			long time = System.currentTimeMillis() - Elasql.SYSTEM_INIT_TIME_MS;
			logger.info(String.format("a new migration starts at %d. New Plan: %s"
					, time / 1000, newPartPlan));
		}
		
		currentPhase = initialPhase;
		migrationRanges = generateMigrationRanges(newPartPlan);
		for (MigrationRange range : migrationRanges)
			if (range.getDestPartId() == Elasql.serverId())
				pushRanges.add(range);
		newPartitionPlan = newPartPlan;
		
		if (!pushRanges.isEmpty())
			scheduleNextBGPushRequest();
		
		if (logger.isLoggable(Level.INFO)) {
			logger.info(String.format("migration ranges: %s", migrationRanges.toString()));
		}
	}
	
	public void scheduleNextBGPushRequest() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				for (MigrationRange range : pushRanges) {
					Set<RecordKey> chunk = range.generateNextMigrationChunk(CHUNK_SIZE);
					if (chunk.size() > 0) {
						sendBGPushRequest(chunk, range.getSourcePartId(), 
								range.getDestPartId());
						lastChunk = chunk;
						lastUpdate = range.generateStatusUpdate();
						return;
					}
				}
				
				if (lastChunk != null) {
					sendBGPushRequest(new HashSet<RecordKey>(), lastUpdate.getSourcePartId(),
							lastUpdate.getDestPartId());
					lastChunk = null;
					lastUpdate = null;
					return;
				}	
				
				// If it reach here, it means that there is no more chunk
				sendRangeFinishNotification();
			}
		}).start();
	}
	
	public void sendBGPushRequest(Set<RecordKey> chunk, int sourceNodeId, int destNodeId) {
		if (logger.isLoggable(Level.INFO))
			logger.info("send a background push request with " + chunk.size() + " keys.");
		
		// Prepare the parameters
		Object[] params;
		if (lastChunk == null) {
			params = new Object[5 + chunk.size()];
		} else {
			params = new Object[5 + chunk.size() + lastChunk.size()];
		}
		
		params[0] = lastUpdate;
		params[1] = sourceNodeId;
		params[2] = destNodeId;
		
		params[3] = chunk.size();
		
		int i = 4;
		for (RecordKey key : chunk)
			params[i++] = key;
		
		if (lastChunk == null) {
			params[i++] = 0;
		} else {
			params[i++] = lastChunk.size();
			for (RecordKey key : lastChunk)
				params[i++] = key;
		}
		
		// Send a store procedure call
		Object[] call = { new StoredProcedureCall(-1, -1, 
				MigrationStoredProcFactory.SP_BG_PUSH, params)};
		Elasql.connectionMgr().sendBroadcastRequest(call, false);
	}
	
	public void updateMigrationRange(MigrationRangeUpdate update) {
		for (MigrationRange range : migrationRanges)
			if (range.updateMigrationStatus(update))
				return;
		throw new RuntimeException(String.format("This is no match for the update", update));
	}
	
	public void sendRangeFinishNotification() {
		if (logger.isLoggable(Level.INFO))
			logger.info("send a range finish notification to the system controller");
		
		TupleSet ts = new TupleSet(MigrationSystemController.MSG_RANGE_FINISH);
		ts.setMetadata(new Serializable[] {pushRanges.size()}); // notify how many ranges are migrated
		Elasql.connectionMgr().pushTupleSet(MigrationSystemController.CONTROLLER_NODE_ID, ts);
	}
	
	public void finishMigration() {
		if (logger.isLoggable(Level.INFO)) {
			long time = System.currentTimeMillis() - Elasql.SYSTEM_INIT_TIME_MS;
			logger.info(String.format("the migration finishes at %d."
					, time / 1000));
		}
		
		// Change the current partition plan of the system
		Elasql.partitionMetaMgr().setNewPartitionPlan(newPartitionPlan);
		
		// Clear the migration states
		currentPhase = Phase.NORMAL;
		migrationRanges.clear();
		pushRanges.clear();
	}
	
	public boolean isMigratingRecord(RecordKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return true;
		return false;
	}
	
	public boolean isMigrated(RecordKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return range.isMigrated(key);
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public void setMigrated(RecordKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key)) {
				range.setMigrated(key);
				return;
			}
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public int checkSourceNode(RecordKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return range.getSourcePartId();
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public int checkDestNode(RecordKey key) {
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
		return new CrabbingAnalyzer();
	}
	
	public abstract List<MigrationRange> generateMigrationRanges(PartitionPlan newPlan);
}
