package org.elasql.migration.mgcrab;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.MigrationComponentFactory;
import org.elasql.migration.MigrationPlan;
import org.elasql.migration.MigrationRange;
import org.elasql.migration.MigrationStoredProcFactory;
import org.elasql.migration.MigrationSystemController;
import org.elasql.server.Elasql;

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
	protected void executeMigration(MigrationPlan plan) throws InterruptedException {
		// Trigger a migration
		if (logger.isLoggable(Level.INFO)) {
			logger.info("Triggers a migration. The plan is: " + plan.toString());
		}
		
		sendMigrationStartRequest(plan);
		List<MigrationRange> ranges = plan.getMigrationRanges(comsFactory);
		numOfRangesToBeMigrated.set(ranges.size());
		
		// Caught up phase
		if (MgcrabSettings.ENABLE_CAUGHT_UP) {
			// Wait for some time
			try {
				Thread.sleep(MgcrabSettings.START_CAUGHT_UP_DELAY);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			sendCaughtUpModeRequest();
		}
		
		// Wait for finish of migrations
		while (numOfRangesToBeMigrated.get() > 0) {
			synchronized (migrationLock) {
				migrationLock.wait();
			}
		}
		
		// Send the migration finish request
		sendMigrationFinishRequest();
	}
	
	@Override
	public void sendMigrationStartRequest(MigrationPlan plan) {
		if (logger.isLoggable(Level.INFO))
			logger.info("send a MigrationStart request.");
		
		// Send a store procedure call
		Object[] params = new Object[] { plan, MgcrabSettings.INIT_PHASE };
		Elasql.connectionMgr().sendStoredProcedureCall(false, 
				MigrationStoredProcFactory.SP_MIGRATION_START, params);
	}
	
	public void sendCaughtUpModeRequest() {
		if (logger.isLoggable(Level.INFO))
			logger.info("send a caught-up phase request.");
		
		// Send a store procedure call
		Object[] params = new Object[] { Phase.CAUGHT_UP };
		Elasql.connectionMgr().sendStoredProcedureCall(false, 
				MgCrabStoredProcFactory.SP_PHASE_CHANGE, params);
	}
}
