package org.elasql.migration;

import java.util.Deque;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.NotificationPartitionPlan;
import org.elasql.storage.metadata.PartitionPlan;
import org.elasql.storage.metadata.RangePartitionPlan;
import org.elasql.storage.metadata.ScalingInPartitionPlan;
import org.elasql.storage.metadata.ScalingOutPartitionPlan;

public abstract class MigrationMgr {
	private static Logger logger = Logger.getLogger(MigrationMgr.class.getName());
	
	public static final boolean ENABLE_NODE_SCALING = false;
	public static final boolean IS_SCALING_OUT = false;
	public static final boolean ENABLE_COLD_MIGRATION = false;
	public static final boolean USE_RANGE_SCALING_OUT = false; // only works when ENABLE_NODE_SCALING && IS_SCALING_OUT = true
	
	public static final int SP_MIGRATION_START = -101;
	public static final int SP_COLD_MIGRATION = -102;
	public static final int SP_MIGRATION_END = -103;
	
	public static final int MSG_COLD_FINISH = -100;
	
	private static final int CHUNK_SIZE = 100;
	
	private static final long START_MIGRATION_TIME = 120_000; // in ms
	private static final long COLD_MIGRATION_DELAY = 0_000; // in ms
	
	private Deque<MigrationRange> targetRanges;
	
	// ======== Functions for Sequencer Node =========
	
	public void startMigrationTrigger() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Starts migration trigger thread.");
		
		if (!ENABLE_NODE_SCALING)
			return;
		
		new Thread(new Runnable() {

			@Override
			public void run() {
				// Wait for some time
				try {
					Thread.sleep(START_MIGRATION_TIME);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				if (logger.isLoggable(Level.INFO))
					logger.info("Triggers a migration.");
				
				// Trigger a new migration
				NotificationPartitionPlan wrapperPlan = (NotificationPartitionPlan)
						Elasql.partitionMetaMgr().getPartitionPlan();
				RangePartitionPlan oldPlan = (RangePartitionPlan) wrapperPlan.getUnderlayerPlan();
				PartitionPlan newPlan;
				if (IS_SCALING_OUT) {
					if (USE_RANGE_SCALING_OUT)
						newPlan = oldPlan.scaleOutRangePartition();
					else
						newPlan = oldPlan.scaleOut();
				} else
					newPlan = oldPlan.scaleIn();
				
				sendMigrationStartRequest(oldPlan, newPlan, getMigrationTableName(), false);
				initializeMigration(oldPlan, newPlan, getMigrationTableName());
				
				// Postpone the cold migration forever
				if (!ENABLE_COLD_MIGRATION)
					return;
				
				// Delay cold migration a moment to prevent a big drop caused by 
				// both initialization and cold migrations.
				try {
					Thread.sleep(COLD_MIGRATION_DELAY);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				// Choose a range to migrate
				MigrationRange range = takeNextMigrationChunk();
				sendColdMigrationRequest(range, false);
			}
			
		}).start();
	}
	
	// Called by Appia thread
	public void onReceiveColdMigrationFinish() {
		MigrationRange range = takeNextMigrationChunk();
		if (range != null)
			sendColdMigrationRequest(range, true);
		else {
			sendMigrationFinishRequest(true);
			finishMigration();
		}
	}
	
	public void sendMigrationStartRequest(PartitionPlan oldPlan,
			PartitionPlan newPlan, String targetTable, boolean isAppiaThread) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Send a MigrationStart request.");
		
		// Send a store procedure call
		Object[] params = new Object[] {oldPlan, newPlan, targetTable};
		Object[] call = { new StoredProcedureCall(-1, -1, SP_MIGRATION_START, params)};
		Elasql.connectionMgr().sendBroadcastRequest(call, isAppiaThread);
	}
	
	public void sendColdMigrationRequest(MigrationRange range, boolean isAppiaThread) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Send a ColdMigration request, range: " + range + ".");
		
		// Send a store procedure call
		Object[] params = new Object[] {range};
		Object[] call = { new StoredProcedureCall(-1, -1, SP_COLD_MIGRATION, params)};
		Elasql.connectionMgr().sendBroadcastRequest(call, isAppiaThread);
	}
	
	public void sendMigrationFinishRequest(boolean isAppiaThread) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Send a MigrationFinish request.");
		
		// Send a store procedure call
		Object[] params = null;
		Object[] call = { new StoredProcedureCall(-1, -1, SP_MIGRATION_END, params)};
		Elasql.connectionMgr().sendBroadcastRequest(call, isAppiaThread);
	}
	
	// ======== Functions for Scheduler on Each Node =========
	
	// Currently, it can only handle range partitioning
	public void initializeMigration(PartitionPlan oldPartPlan,
			PartitionPlan newPartPlan, String targetTable) {
		if (logger.isLoggable(Level.INFO)) {
			long time = System.currentTimeMillis() - Elasql.START_TIME_MS;
			logger.info(String.format("a new migration starts at %d. Old: %s, New: %s"
					, time / 1000, oldPartPlan, newPartPlan));
		}
		
		// Analyze the migration plans to find out which ranges to migrate
		if (IS_SCALING_OUT) {
			RangePartitionPlan originalPlan = (RangePartitionPlan) oldPartPlan;
			if (USE_RANGE_SCALING_OUT) {
				RangePartitionPlan scalingOutPlan = (RangePartitionPlan) newPartPlan;
				targetRanges = scalingOutPlan.generateMigrationRanges(originalPlan, targetTable);
			} else {
				ScalingOutPartitionPlan scalingOutPlan = (ScalingOutPartitionPlan) newPartPlan;
				targetRanges = scalingOutPlan.generateMigrationRanges(originalPlan, targetTable);
			}
		} else {
			RangePartitionPlan originalPlan = (RangePartitionPlan) oldPartPlan;
			ScalingInPartitionPlan scalingInPlan = (ScalingInPartitionPlan) newPartPlan;
			targetRanges = scalingInPlan.generateMigrationRanges(originalPlan, targetTable);
		}
		
		System.out.println("Ranges: " + targetRanges);
		
		// Change the current partition plan of the system
		Elasql.partitionMetaMgr().startMigration(newPartPlan);
	}
	
	public void markMigrationRangeMoved(MigrationRange range) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Range: " + range + " is marked as migrated.");
		
		MigrationRange selfRange = takeNextMigrationChunk();
		if (!selfRange.equals(range))
			throw new RuntimeException("the migration ranges do not match !\n" +
					"Request range: " + range + "\nLocal range: " + selfRange);
	}
	
	public void finishMigration() {
		// Check if there is no range
		if (!targetRanges.isEmpty())
			throw new RuntimeException("the migration is broken.");
		
		// Clear all metadata
		Elasql.partitionMetaMgr().finishMigration();
		targetRanges = null;
		
		if (logger.isLoggable(Level.INFO)) {
			long time = System.currentTimeMillis() - Elasql.START_TIME_MS;
			logger.info(String.format("the migration finishes at %d."
					, time / 1000));
		}
	}
	
	public Integer getSourcePart(RecordKey key) {
		int id = toNumericId(key);
		for (MigrationRange range : targetRanges)
			if (range.contains(id))
				return range.getSourcePartId();
		return null;
	}
	
	// ======== Utility Functions =========
	
	public abstract String getMigrationTableName();
	
	public abstract Iterator<RecordKey> toKeyIterator(MigrationRange range);
	
	public abstract int toNumericId(RecordKey key);
	
	// ======== Private Functions =========
	
	private MigrationRange takeNextMigrationChunk() {
		if (targetRanges.isEmpty())
			return null;
		
		MigrationRange coarseRange = targetRanges.getFirst();
		MigrationRange chunkRange = coarseRange.cutASlice(CHUNK_SIZE);
		if (chunkRange == null)
			chunkRange = targetRanges.removeFirst();
		return chunkRange;
	}
}
