package org.elasql.migration;

import java.util.Deque;
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionPlan;
import org.elasql.storage.metadata.ScalablePartitionPlan;
import org.elasql.util.ElasqlProperties;

public abstract class MigrationMgr {
	private static Logger logger = Logger.getLogger(MigrationMgr.class.getName());
	
	public static final boolean ENABLE_NODE_SCALING;
	public static final boolean IS_SCALING_OUT; // Scaling-out or consolidation
	public static final boolean ENABLE_COLD_MIGRATION; // Only add/remove machines or proactively migrates
	public static final boolean USE_RANGE_SCALING_OUT; // only works when ENABLE_NODE_SCALING && IS_SCALING_OUT = true
	
	static {
		ENABLE_NODE_SCALING = ElasqlProperties.getLoader()
				.getPropertyAsBoolean(MigrationMgr.class.getName() + ".ENABLE_NODE_SCALING", false);
		IS_SCALING_OUT = ElasqlProperties.getLoader()
				.getPropertyAsBoolean(MigrationMgr.class.getName() + ".IS_SCALING_OUT", true);
		ENABLE_COLD_MIGRATION = ElasqlProperties.getLoader()
				.getPropertyAsBoolean(MigrationMgr.class.getName() + ".ENABLE_COLD_MIGRATION", true);
		USE_RANGE_SCALING_OUT = ElasqlProperties.getLoader()
				.getPropertyAsBoolean(MigrationMgr.class.getName() + ".USE_RANGE_SCALING_OUT", false);
	}
	
	public static final int SP_MIGRATION_START = -101;
	public static final int SP_COLD_MIGRATION = -102;
	public static final int SP_MIGRATION_END = -103;
	
	public static final int MSG_COLD_FINISH = -100;
	
	private static final int CHUNK_SIZE = 1000;
	
	private static final long START_MIGRATION_TIME = 300_000; // in ms
//	private static final long START_MIGRATION_TIME = 100_000_000; // in ms
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
				PartitionPlan partPlan = Elasql.partitionMetaMgr().getPartitionPlan();
				ScalablePartitionPlan scalePlan = (ScalablePartitionPlan) partPlan.getBasePartitionPlan();
				MigrationPlan migratePlan;
				if (IS_SCALING_OUT) {
					migratePlan = scalePlan.scaleOut();
				} else
					migratePlan = scalePlan.scaleIn();
				
				sendMigrationStartRequest(migratePlan, false);
				initializeMigration(migratePlan);
				
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
	
	public void sendMigrationStartRequest(MigrationPlan migratePlan, boolean isAppiaThread) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Send a MigrationStart request.");
		
		// Send a store procedure call
		Object[] params = new Object[] {migratePlan};
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
	public void initializeMigration(MigrationPlan migratePlan) {
		if (logger.isLoggable(Level.INFO)) {
			long time = System.currentTimeMillis() - Elasql.START_TIME_MS;
			logger.info(String.format("a new migration starts at %d from %s"
					+ " to %s.", time / 1000, migratePlan.oldPartitionPlan(),
					migratePlan.newPartitionPlan()));
		}
		
		// Analyze the migration plans to find out which ranges to migrate
		targetRanges = migratePlan.generateMigrationRanges();

		if (logger.isLoggable(Level.INFO))
			logger.info(String.format("migration ranges: %s", targetRanges));
		
		// Change the current partition plan of the system
		Elasql.partitionMetaMgr().startMigration(migratePlan.newPartitionPlan());
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
	
	/**
	 * Get the partition id of a migrating record. Return
	 * null if the record is not in the migration range or
	 * the record has been migrated.
	 * 
	 * @param key the key of the specified record
	 * @return the id of the partition owning the record
	 */
	public Integer getPartition(RecordKey key) {
		int id = toNumericId(key);
		for (MigrationRange range : targetRanges)
			if (range.contains(id))
				return range.getSourcePartId();
		return null;
	}
	
	// ======== Utility Functions =========
	
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
