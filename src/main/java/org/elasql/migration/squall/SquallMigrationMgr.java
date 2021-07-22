package org.elasql.migration.squall;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.migration.MigrationComponentFactory;
import org.elasql.migration.MigrationMgr;
import org.elasql.migration.MigrationPlan;
import org.elasql.migration.MigrationRange;
import org.elasql.migration.MigrationRangeFinishMessage;
import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.migration.MigrationSettings;
import org.elasql.migration.MigrationSystemController;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.schedule.calvin.CalvinScheduler;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.schedule.calvin.squall.SquallAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.storage.tx.Transaction;

public class SquallMigrationMgr implements MigrationMgr {
	private static Logger logger = Logger.getLogger(SquallMigrationMgr.class.getName());
	
	private List<MigrationRange> migrationRanges;
	private List<MigrationRange> pushRanges = new ArrayList<MigrationRange>(); // the ranges whose destination is this node.
	private PartitionPlan newPartitionPlan;
	private boolean isInMigration;
	private MigrationComponentFactory comsFactory;
	
	public SquallMigrationMgr(MigrationComponentFactory comsFactory) {
		this.comsFactory = comsFactory;
	}

	public void initializeMigration(Transaction tx, MigrationPlan plan, Object[] params) {
		PartitionPlan newPartPlan = plan.getNewPart();
		
		if (logger.isLoggable(Level.INFO)) {
			long time = System.currentTimeMillis() - CalvinScheduler.FIRST_TX_ARRIVAL_TIME.get();
			PartitionPlan currentPartPlan = Elasql.partitionMetaMgr().getPartitionPlan();
			logger.info(String.format("a new migration starts at %d. Current Plan: %s, New Plan: %s"
					, time / 1000, currentPartPlan, newPartPlan));
		}
		
		// Initialize states
		isInMigration = true;
		migrationRanges = plan.getMigrationRanges(comsFactory);
		for (MigrationRange range : migrationRanges)
			if (range.getDestPartId() == Elasql.serverId())
				pushRanges.add(range);
		newPartitionPlan = newPartPlan;
		
		if (!pushRanges.isEmpty())
			scheduleNextBGPushRequest();
		
		if (logger.isLoggable(Level.INFO)) {
			logger.info(String.format("migration ranges: %s", migrationRanges.toString()));
		}
	}
	
	public void scheduleNextBGPushRequest() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				for (MigrationRange range : pushRanges) {
					Set<PrimaryKey> chunk = range.generateNextMigrationChunk(
							MigrationSettings.USE_BYTES_FOR_CHUNK_SIZE, MigrationSettings.CHUNK_SIZE);
					
					// Debug
//					System.out.println("Generated a chunk: " + chunk);
					
					if (chunk.size() > 0) {
						sendBGPushRequest(range.generateStatusUpdate(), chunk, 
								range.getSourcePartId(), range.getDestPartId());
						return;
					}
				}
				
				// If it reach here, it means that there is no more chunk
				sendRangeFinishNotification();
			}
		}).start();
	}
	
	public void sendBGPushRequest(MigrationRangeUpdate update, Set<PrimaryKey> chunk,
			int sourceNodeId, int destNodeId) {
		if (logger.isLoggable(Level.INFO))
			logger.info("send a background push request with " + chunk.size() + " keys.");
		
		// Prepare the parameters
		Object[] params = new Object[4 + chunk.size()];
		
		params[0] = update;
		params[1] = sourceNodeId;
		params[2] = destNodeId;
		params[3] = chunk.size();
		int i = 4;
		for (PrimaryKey key : chunk)
			params[i++] = key;
		
		// Send a store procedure call
		Elasql.connectionMgr().sendStoredProcedureCall(false, 
				SquallStoredProcFactory.SP_BG_PUSH, params);
	}
	
	public void addNewInsertKeyOnSource(PrimaryKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.addKey(key))
				return;
		throw new RuntimeException(String.format("This is no match for the key: %s", key));
	}
	
	public void updateMigrationRange(MigrationRangeUpdate update) {
		for (MigrationRange range : migrationRanges)
			if (range.updateMigrationStatus(update))
				return;
		throw new RuntimeException(String.format("This is no match for the update: %s", update));
	}
	
	public void sendRangeFinishNotification() {
		if (logger.isLoggable(Level.INFO))
			logger.info("send a range finish notification to the system controller");
		
		TupleSet ts = new TupleSet(MigrationSystemController.MSG_RANGE_FINISH);
		ts.setMetadata(new MigrationRangeFinishMessage(pushRanges.size())); // notify how many ranges are migrated
		Elasql.connectionMgr().pushTupleSet(MigrationSystemController.CONTROLLER_NODE_ID, ts);
	}
	
	public void finishMigration(Transaction tx, Object[] params) {
		if (logger.isLoggable(Level.INFO)) {
			long time = System.currentTimeMillis() - CalvinScheduler.FIRST_TX_ARRIVAL_TIME.get();
			logger.info(String.format("the migration finishes at %d."
					, time / 1000));
		}
		
		// Change the current partition plan of the system
		Elasql.partitionMetaMgr().setNewPartitionPlan(newPartitionPlan);
		
		// Clear the migration states
		isInMigration = false;
		migrationRanges.clear();
		pushRanges.clear();
	}
	
	public boolean isMigratingRecord(PrimaryKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return true;
		return false;
	}
	
	public boolean isMigrated(PrimaryKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return range.isMigrated(key);
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public void setMigrated(PrimaryKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key)) {
				range.setMigrated(key);
				return;
			}
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public int checkSourceNode(PrimaryKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return range.getSourcePartId();
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public int checkDestNode(PrimaryKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return range.getDestPartId();
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public boolean isInMigration() {
		return isInMigration;
	}
	
	public ReadWriteSetAnalyzer newAnalyzer() {
		return new SquallAnalyzer();
	}
}
