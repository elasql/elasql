package org.elasql.migration;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.server.Elasql;
import org.elasql.storage.metadata.NotificationPartitionPlan;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.metadata.PartitionPlan;

public class MigrationSystemController {
	private static Logger logger = Logger.getLogger(MigrationSystemController.class.getName());
	
	public static final int MSG_RANGE_FINISH = -8787;
	public static final int CONTROLLER_NODE_ID = PartitionMetaMgr.NUM_PARTITIONS;
	
	protected AtomicInteger numOfRangesToBeMigrated = new AtomicInteger(0);
	protected MigrationComponentFactory comsFactory;
	
	public MigrationSystemController(MigrationComponentFactory comsFactory) {
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
			}
			
		}).start();
	}
	
	public void sendMigrationStartRequest(PartitionPlan newPartPlan) {
		if (logger.isLoggable(Level.INFO))
			logger.info("send a MigrationStart request.");
		
		// Send a store procedure call
		Object[] params = new Object[] {newPartPlan};
		Elasql.connectionMgr().sendStoredProcedureCall(false, 
				MigrationStoredProcFactory.SP_MIGRATION_START, params);
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
