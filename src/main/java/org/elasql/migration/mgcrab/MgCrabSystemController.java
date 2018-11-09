package org.elasql.migration.mgcrab;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.MigrationComponentFactory;
import org.elasql.migration.MigrationRange;
import org.elasql.migration.MigrationRangeFinishMessage;
import org.elasql.migration.MigrationStoredProcFactory;
import org.elasql.migration.MigrationSystemController;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.server.Elasql;
import org.elasql.storage.metadata.NotificationPartitionPlan;
import org.elasql.storage.metadata.PartitionPlan;

/**
 * The system controller only exists in the sequencer node. Job: 
 * (1) initializes a migration,
 * (2) notifies of phase changing, and
 * (3) finishes a migration.
 */
public class MgCrabSystemController implements MigrationSystemController {
	private static Logger logger = Logger.getLogger(MgCrabSystemController.class.getName());
	
	private static final Phase INIT_PHASE = Phase.CRABBING;
	
	private static final long START_CAUGHT_UP_DELAY = 90_000; // multi-indices
//	private static final long START_CAUGHT_UP_DELAY = 500_000; // normal
//	private static final long START_CAUGHT_UP_DELAY = 5000_000; // long enough to disable
	
	private AtomicInteger numOfRangesToBeMigrated = new AtomicInteger(0);
	
	private MigrationComponentFactory comsFactory;
	
	public MgCrabSystemController(MigrationComponentFactory comsFactory) {
		if (logger.isLoggable(Level.INFO))
			logger.info("the system controller is ready");
		
		this.comsFactory = comsFactory;
		if (ENABLE_MIGRATION)
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
				
				PartitionPlan newPartPlan = comsFactory.newPartitionPlan();
				sendMigrationStartRequest(newPartPlan);
				
				// Determine how many ranges should be migrated
				PartitionPlan currentPlan = Elasql.partitionMetaMgr().getPartitionPlan();
				if (currentPlan.getClass().equals(NotificationPartitionPlan.class))
					currentPlan = ((NotificationPartitionPlan) currentPlan).getUnderlayerPlan();
				List<MigrationRange> ranges = comsFactory.generateMigrationRanges(currentPlan, newPartPlan);
				numOfRangesToBeMigrated.set(ranges.size());
				
				// Wait for some time
				try {
					Thread.sleep(START_CAUGHT_UP_DELAY);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
				
				sendCaughtUpModeRequest();
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
	
	public void sendCaughtUpModeRequest() {
		if (logger.isLoggable(Level.INFO))
			logger.info("send a caught-up phase request.");
		
		// Send a store procedure call
		Object[] params = new Object[] {Phase.CAUGHT_UP};
		Object[] call = { new StoredProcedureCall(-1, -1, 
				MgCrabStoredProcFactory.SP_PHASE_CHANGE, params)};
		Elasql.connectionMgr().sendBroadcastRequest(call, false);
	}
	
	public void onReceiveMigrationRangeFinishMsg(MigrationRangeFinishMessage msg) {
		int currentLeft = numOfRangesToBeMigrated.get();
		int newCount = currentLeft - msg.getFinishRangeCount();
		while (!numOfRangesToBeMigrated.compareAndSet(currentLeft, newCount)) {
			currentLeft = numOfRangesToBeMigrated.get();
			newCount = currentLeft - msg.getFinishRangeCount();
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
}
