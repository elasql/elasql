package org.elasql.migration.mgcrab;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.MigrationComponentFactory;
import org.elasql.migration.MigrationRange;
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
public class MgCrabSystemController extends MigrationSystemController {
	private static Logger logger = Logger.getLogger(MgCrabSystemController.class.getName());
	
	public MgCrabSystemController(MigrationComponentFactory comsFactory) {
		super(comsFactory);
	}

	@Override
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
	
	@Override
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
}
