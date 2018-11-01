package org.elasql.migration;

import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.sp.MigrationStoredProcFactory;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.server.Elasql;
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
				
				sendMigrationStartRequest();
			}
			
		}).start();
	}
	
	public void sendMigrationStartRequest() {
		if (logger.isLoggable(Level.INFO))
			logger.info("send a MigrationStart request.");
		
		// Send a store procedure call
		Object[] params = new Object[] {newPartitionPlan(), INIT_PHASE};
		Object[] call = { new StoredProcedureCall(-1, -1, 
				MigrationStoredProcFactory.SP_MIGRATION_START, params)};
		Elasql.connectionMgr().sendBroadcastRequest(call, false);
	}
	
	public void onReceiveMigrationRangeFinishMsg() {
		// TODO
	}
	
	public void sendMigrationFinishRequest() {
		// TODO
	}
	
	public abstract PartitionPlan newPartitionPlan();
}
