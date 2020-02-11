package org.elasql.migration.mgcrab;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.MigrationComponentFactory;
import org.elasql.migration.MigrationRange;
import org.elasql.migration.MigrationRangeFinishMessage;
import org.elasql.migration.MigrationSettings;
import org.elasql.migration.MigrationStoredProcFactory;
import org.elasql.migration.MigrationSystemController;
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
	
	private AtomicInteger numOfRangesToBeMigrated = new AtomicInteger(0);
	
	private MigrationComponentFactory comsFactory;
	
	public MgCrabSystemController(MigrationComponentFactory comsFactory) {
		if (logger.isLoggable(Level.INFO))
			logger.info("the system controller is ready");
		
		this.comsFactory = comsFactory;
		if (MigrationSettings.ENABLE_MIGRATION)
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
					Thread.sleep(MigrationSettings.START_MIGRATION_TIME);
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
				
				if (!MgcrabSettings.ENABLE_CAUGHT_UP)
					return;
				
				// Wait for some time
				try {
					Thread.sleep(MgcrabSettings.START_CAUGHT_UP_DELAY);
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
		Object[] params = new Object[] {newPartPlan, MgcrabSettings.INIT_PHASE};
		Elasql.connectionMgr().sendStoredProcedureCall(false, 
				MigrationStoredProcFactory.SP_MIGRATION_START, params);
	}
	
	public void sendCaughtUpModeRequest() {
		if (logger.isLoggable(Level.INFO))
			logger.info("send a caught-up phase request.");
		
		// Send a store procedure call
		Object[] params = new Object[] {Phase.CAUGHT_UP};
		Elasql.connectionMgr().sendStoredProcedureCall(false, 
				MgCrabStoredProcFactory.SP_PHASE_CHANGE, params);
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
		Elasql.connectionMgr().sendStoredProcedureCall(true, 
				MigrationStoredProcFactory.SP_MIGRATION_END, params);
	}
}
