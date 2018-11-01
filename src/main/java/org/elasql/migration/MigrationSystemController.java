package org.elasql.migration;

import java.io.Serializable;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.sp.MigrationStoredProcFactory;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.server.Elasql;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.metadata.PartitionPlan;

/**
 * The system controller only exists in the sequencer node. Job: 
 * (1) initializes a migration,
 * (2) notifies of phase changing, and
 * (3) finishes a migration.
 */
public abstract class MigrationSystemController {
	private static Logger logger = Logger.getLogger(MigrationSystemController.class.getName());
	
	private static final Phase INIT_PHASE = Phase.CRABBING;
	
	private static final long START_MIGRATION_TIME = 180_000;
//	private static final long START_MIGRATION_TIME = 5000_000;
	
	public static final int MSG_RANGE_FINISH = -8787;
	public static final int CONTROLLER_NODE_ID = PartitionMetaMgr.NUM_PARTITIONS;
	
	private AtomicInteger numOfRangesToBeMigrated = new AtomicInteger(0);
	
	public MigrationSystemController() {
		if (logger.isLoggable(Level.INFO))
			logger.info("the system controller is ready");
		
		startMigrationTrigger();
	}

	public void startMigrationTrigger() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Starts migration trigger thread.");
		
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
				
				PartitionPlan newPartPlan = newPartitionPlan();
				sendMigrationStartRequest(newPartPlan);
				
				// Determine how many ranges should be migrated
				List<MigrationRange> ranges = Elasql.migrationMgr().generateMigrationRanges(newPartPlan);
				numOfRangesToBeMigrated.set(ranges.size());
			}
			
		}).start();
	}
	
	public void sendMigrationStartRequest(PartitionPlan newPartPlan) {
		if (logger.isLoggable(Level.INFO))
			logger.info("send a MigrationStart request.");
		
		// Send a store procedure call
		Object[] params = new Object[] {newPartPlan, INIT_PHASE};
		Object[] call = { new StoredProcedureCall(-1, -1, 
				MigrationStoredProcFactory.SP_MIGRATION_START, params)};
		Elasql.connectionMgr().sendBroadcastRequest(call, false);
	}
	
	public void onReceiveMigrationRangeFinishMsg(Serializable[] message) {
		int count = (Integer) message[0];
		int currentLeft = numOfRangesToBeMigrated.get();
		int newCount = currentLeft - count;
		while (!numOfRangesToBeMigrated.compareAndSet(currentLeft, newCount)) {
			currentLeft = numOfRangesToBeMigrated.get();
			newCount = currentLeft - count;
		}
		
		if (numOfRangesToBeMigrated.get() == 0)
			sendMigrationFinishRequest();
		else {
			if (logger.isLoggable(Level.INFO))
				logger.info("got a migration range finish notification. " +
						newCount + " ranges are left to go.");
		}
	}
	
	public void sendMigrationFinishRequest() {
		if (logger.isLoggable(Level.INFO))
			logger.info("send a MigrationFinish request.");
		
		// Send a store procedure call
		Object[] params = new Object[] {};
		Object[] call = { new StoredProcedureCall(-1, -1, 
				MigrationStoredProcFactory.SP_MIGRATION_END, params)};
		Elasql.connectionMgr().sendBroadcastRequest(call, true);
	}
	
	public abstract PartitionPlan newPartitionPlan();
}
