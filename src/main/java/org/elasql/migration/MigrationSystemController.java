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

public class MigrationSystemController implements Runnable {
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
	protected Object migrationLock = new Object();
	
	public MigrationSystemController(MigrationComponentFactory comsFactory) {
		this.comsFactory = comsFactory;
		if (MigrationSettings.ENABLE_MIGRATION) {
			if (logger.isLoggable(Level.INFO))
				logger.info("Start the migration controller");
			
			new Thread(this).start();
		}
	}
	
	public void monitorTransaction(Set<RecordKey> reads, Set<RecordKey> writes) {
		workloadFeeds.add(new TransactionInfo(reads, writes));
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
				planner.reset();
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
				List<MigrationRange> ranges = plan.getMigrationRanges();
				numOfRangesToBeMigrated.set(ranges.size());
				
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
			sendMigrationFinishRequest();
			
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
}
