package org.elasql.migration.stopcopy;

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

public class StopCopySystemController implements MigrationSystemController {
	private static Logger logger = Logger.getLogger(StopCopySystemController.class.getName());
	
	private AtomicInteger numOfRangesToBeMigrated = new AtomicInteger(0);
	
	private MigrationComponentFactory comsFactory;
	
	public StopCopySystemController(MigrationComponentFactory comsFactory) {
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
			}
			
		}).start();
	}
	
	public void sendMigrationStartRequest(PartitionPlan newPartPlan) {
		if (logger.isLoggable(Level.INFO))
			logger.info("send a MigrationStart request.");
		
		// Send a store procedure call
		Object[] params = new Object[] {newPartPlan};
		Object[] call = { new StoredProcedureCall(-1, -1, 
				MigrationStoredProcFactory.SP_MIGRATION_START, params)};
		Elasql.connectionMgr().sendBroadcastRequest(call, false);
	}
	
	public void onReceiveMigrationRangeFinishMsg(MigrationRangeFinishMessage msg) {
		// do nothing
	}
	
	public void sendMigrationFinishRequest() {
		// do nothing
	}
}