package org.elasql.migration.mgcrab;

import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.MigrationComponentFactory;
import org.elasql.migration.MigrationPlan;
import org.elasql.migration.MigrationRange;
import org.elasql.migration.MigrationSettings;
import org.elasql.migration.MigrationStoredProcFactory;
import org.elasql.migration.MigrationSystemController;
import org.elasql.migration.planner.MigrationPlanner;
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
	public void run() {
		// Wait for some time
		try {
			Thread.sleep(MigrationSettings.START_MONITOR_TIME);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		// Periodically monitor, plan and trigger migrations
		MigrationPlanner planner = comsFactory.newMigrationPlanner();
		while (true) {
			try {
				// Reset the workload logs
				workloadFeeds.clear();

				// Monitor transactions
				long startMonitorTime = System.currentTimeMillis();
				while (System.currentTimeMillis() - startMonitorTime <
						MigrationSettings.MIGRATION_PERIOD) {
					TransactionInfo info = workloadFeeds.take();
					planner.monitorTransaction(info.reads, info.writes);
				}
				
				// Generate migration plans
				MigrationPlan plan = planner.generateMigrationPlan();
				if (plan == null)
					continue;
				
				// Trigger a migration
				if (logger.isLoggable(Level.INFO))
					logger.info("Triggers a migration.");
				
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
			} catch (InterruptedException e) {
				e.printStackTrace();
				return;
			}
		}
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
		Object[] params = new Object[] {Phase.CAUGHT_UP};
		Elasql.connectionMgr().sendStoredProcedureCall(false, 
				MgCrabStoredProcFactory.SP_PHASE_CHANGE, params);
	}
}
