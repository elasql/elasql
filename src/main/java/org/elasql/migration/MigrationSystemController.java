package org.elasql.migration;

import java.util.List;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.planner.MigrationPlanner;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.server.task.Task;

public class MigrationSystemController extends Task {
	private static Logger logger = Logger.getLogger(MigrationSystemController.class.getName());
	
	public static final int MSG_RANGE_FINISH = -8787;
	public static final int CONTROLLER_NODE_ID = PartitionMetaMgr.NUM_PARTITIONS;
	
	protected static class TransactionInfo {
		public Set<RecordKey> reads, writes;
		
		public TransactionInfo(Set<RecordKey> reads, Set<RecordKey> writes) {
			this.reads = reads;
			this.writes = writes;
		}
	}
	
	protected AtomicInteger numOfRangesToBeMigrated = new AtomicInteger(0);
	protected MigrationComponentFactory comsFactory;
	protected BlockingQueue<TransactionInfo> workloadFeeds = new LinkedBlockingQueue<TransactionInfo>();
	protected volatile boolean isAcceptingWorkloadFeeds = false;
	protected Object migrationLock = new Object();
	
	public MigrationSystemController(MigrationComponentFactory comsFactory) {
		this.comsFactory = comsFactory;
		if (MigrationSettings.ENABLE_MIGRATION) {
			if (logger.isLoggable(Level.INFO))
				logger.info("Start the migration controller");
			
			VanillaDb.taskMgr().runTask(this);
		}
	}
	
	public void monitorTransaction(Set<RecordKey> reads, Set<RecordKey> writes) {
		if (isAcceptingWorkloadFeeds) {
			workloadFeeds.add(new TransactionInfo(reads, writes));
		}
	}
	
	@Override
	public void run() {
		Thread.currentThread().setName("Migration System Controller");
		
		// Wait for some time
		try {
			Thread.sleep(MigrationSettings.START_MONITOR_TIME);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		// Use either a predefined plan or a migration planner
		if (MigrationSettings.USE_PREDEFINED_PLAN)
			executeMigrationWithPredefinedPlan();
		else
			runMigrationPlanner();
	}
	
	public void sendMigrationStartRequest(MigrationPlan plan) {
		if (logger.isLoggable(Level.INFO))
			logger.info("send a MigrationStart request.");
		
		// Send a store procedure call
		Object[] params = new Object[] { plan };
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
		
		if (numOfRangesToBeMigrated.get() == 0) {
			// Notify the controller thread
			synchronized (migrationLock) {
				migrationLock.notifyAll();
			}
		} else {
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
	
	private void runMigrationPlanner() {
		if (logger.isLoggable(Level.INFO))
			logger.info("The migration controller starts monitoring the workload");
		
		// Periodically monitor, plan and trigger migrations
		MigrationPlanner planner = comsFactory.newMigrationPlanner();
		while (true) {
			try {
				// Reset the workload logs
				planner.reset();
				workloadFeeds.clear();
				isAcceptingWorkloadFeeds = true;

				// Monitor transactions
				long startMonitorTime = System.currentTimeMillis();
				while (System.currentTimeMillis() - startMonitorTime <
						MigrationSettings.MIGRATION_PERIOD) {
					TransactionInfo info = workloadFeeds.take();
					planner.monitorTransaction(info.reads, info.writes);
				}
				isAcceptingWorkloadFeeds = false;
				
				if (logger.isLoggable(Level.INFO))
					logger.info("A monitoring period finished, starting to generate a migration plan.");
				
				// Generate migration plans
				MigrationPlan plan = planner.generateMigrationPlan();
				if (plan == null) {
					if (logger.isLoggable(Level.INFO))
						logger.info("No migration is needed.");
					continue;
				}
				
				executeMigration(plan);
			} catch (InterruptedException e) {
				e.printStackTrace();
				return;
			}
		}
	}
	
	private void executeMigrationWithPredefinedPlan() {
		if (logger.isLoggable(Level.INFO))
			logger.info("Start a migration with the predefined migration plan");
		
		try {
			MigrationPlan plan = comsFactory.newPredefinedMigrationPlan();
			executeMigration(plan);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	private void executeMigration(MigrationPlan plan) throws InterruptedException {
		// Trigger a migration
		if (logger.isLoggable(Level.INFO))
			logger.info("Triggers a migration. The plan is: " + plan.toString());
		
		sendMigrationStartRequest(plan);
		List<MigrationRange> ranges = plan.getMigrationRanges();
		numOfRangesToBeMigrated.set(ranges.size());
		
		// Wait for finish of migrations
		while (numOfRangesToBeMigrated.get() > 0) {
			synchronized (migrationLock) {
				migrationLock.wait();
			}
		}
		
		// Send the migration finish request
		sendMigrationFinishRequest();
	}
}
