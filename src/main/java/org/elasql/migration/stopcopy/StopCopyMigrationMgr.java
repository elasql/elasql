package org.elasql.migration.stopcopy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.VanillaCoreCrud;
import org.elasql.cache.calvin.CalvinCacheMgr;
import org.elasql.cache.calvin.CalvinPostOffice;
import org.elasql.migration.MigrationComponentFactory;
import org.elasql.migration.MigrationMgr;
import org.elasql.migration.MigrationRange;
import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.schedule.calvin.CalvinScheduler;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.NotificationPartitionPlan;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.sql.IntegerConstant;
import org.vanilladb.core.storage.tx.Transaction;

public class StopCopyMigrationMgr implements MigrationMgr {
	private static Logger logger = Logger.getLogger(StopCopyMigrationMgr.class.getName());
	
	private static final int LARGE_CHUNK_SIZE_IN_BYTES = 16_000_000; // 1MB
	private static final int LARGE_CHUNK_SIZE_IN_COUNT = 160000;
	private static final int LARGE_CHUNK_SIZE = USE_BYTES_FOR_CHUNK_SIZE? LARGE_CHUNK_SIZE_IN_BYTES : LARGE_CHUNK_SIZE_IN_COUNT;
	
	private static final Constant FALSE = new IntegerConstant(0);
	private static final Constant TRUE = new IntegerConstant(1);
	
	private List<MigrationRange> sourceRanges = new ArrayList<MigrationRange>();
	private List<MigrationRange> destRanges = new ArrayList<MigrationRange>();
	
	private MigrationComponentFactory comsFactory;
	
	public StopCopyMigrationMgr(MigrationComponentFactory comsFactory) {
		this.comsFactory = comsFactory;
	}
	
	public void initializeMigration(Transaction tx, Object[] params) {
		// Parse parameters
		PartitionPlan newPartPlan = (PartitionPlan) params[0];
		
		if (logger.isLoggable(Level.INFO)) {
			long time = System.currentTimeMillis() - CalvinScheduler.FIRST_TX_ARRIVAL_TIME.get();
			PartitionPlan currentPartPlan = Elasql.partitionMetaMgr().getPartitionPlan();
			logger.info(String.format("a new migration starts at %d with Tx.%d. Current Plan: %s, New Plan: %s"
					, time / 1000, tx.getTransactionNumber(), currentPartPlan, newPartPlan));
		}
		
		// Wait for active transactions finishes
		waitForActiveTransactionFinish();
		
		analyzeResponsibleRanges(newPartPlan);
		
		// Start the migration immediately
		performEagerMigration(tx);
		
		// Change the current partition plan of the system
		Elasql.partitionMetaMgr().setNewPartitionPlan(newPartPlan);
		
		if (logger.isLoggable(Level.INFO)) {
			long time = System.currentTimeMillis() - CalvinScheduler.FIRST_TX_ARRIVAL_TIME.get();
			logger.info(String.format("the migration finishes at %d."
					, time / 1000));
		}
	}
	
	public void waitForActiveTransactionFinish() {
		while (VanillaDb.txMgr().getActiveTxCount() > 1) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			if (logger.isLoggable(Level.INFO)) {
				logger.info(String.format("current active transaction count: %d",
						VanillaDb.txMgr().getActiveTxCount()));
			}
		}
	}
	
	private void analyzeResponsibleRanges(PartitionPlan newPlan) {
		PartitionPlan currentPlan = Elasql.partitionMetaMgr().getPartitionPlan();
		if (currentPlan.getClass().equals(NotificationPartitionPlan.class))
			currentPlan = ((NotificationPartitionPlan) currentPlan).getUnderlayerPlan();
		List<MigrationRange> ranges = comsFactory.generateMigrationRanges(currentPlan, newPlan);
		
		if (logger.isLoggable(Level.INFO)) {
			logger.info(String.format("migration ranges: %s", ranges));
		}
		
		for (MigrationRange range : ranges) {
			if (Elasql.serverId() == range.getSourcePartId()) {
				sourceRanges.add(range);
			} else if (Elasql.serverId() == range.getDestPartId()) {
				destRanges.add(range);
			}
		}
		
		if (logger.isLoggable(Level.INFO)) {
			logger.info(String.format("this node acts as source in %s, and destination in %s",
					sourceRanges, destRanges));
		}
	}
	
	public void performEagerMigration(Transaction tx) {
		if (logger.isLoggable(Level.INFO)) {
			long time = System.currentTimeMillis() - CalvinScheduler.FIRST_TX_ARRIVAL_TIME.get();
			logger.info(String.format("Data pushing starts at %d."
					, time / 1000));
		}
		
		Set<Integer> waitForDests = new HashSet<Integer>();
		int finishSources = 0;
		int finishDests = 0;
		
		// create a CacheMgr
		CalvinPostOffice postOffice = (CalvinPostOffice) Elasql.remoteRecReceiver();
		CalvinCacheMgr cacheMgr = postOffice.createCacheMgr(tx, true);
		
		while (finishSources < sourceRanges.size() || finishDests < destRanges.size()) {
			finishSources = 0;
			finishDests = 0;
			
			for (MigrationRange range : sourceRanges) {
				Set<RecordKey> chunk = range.generateNextMigrationChunk(USE_BYTES_FOR_CHUNK_SIZE, LARGE_CHUNK_SIZE);
				if (chunk.size() > 0) {
					readAndPushADataChunk(tx, chunk, range.getDestPartId());
					waitForDests.add(range.getDestPartId());
				} else {
					finishSources++;
				}
			}
			
			for (MigrationRange range : destRanges) {
				Set<RecordKey> chunk = range.generateNextMigrationChunk(USE_BYTES_FOR_CHUNK_SIZE, LARGE_CHUNK_SIZE);
				if (chunk.size() > 0) {
					receiveAndInsertDataChunk(tx, cacheMgr, chunk);
					sendAnAck(tx, range.getSourcePartId());
				} else {
					finishDests++;
				}
			}
			
			for (MigrationRange range : sourceRanges) {
				if (waitForDests.contains(range.getDestPartId())) {
					waitForAck(tx, cacheMgr, range.getDestPartId());
					waitForDests.remove(range.getDestPartId());
				}
			}
		}
	}
	
	private void readAndPushADataChunk(Transaction tx, Set<RecordKey> pushKeys, int targetNode) {
		// Construct pushing tuple set
		TupleSet ts = new TupleSet(-1);

		// Construct key sets
		Map<String, Set<RecordKey>> keysPerTables = new HashMap<String, Set<RecordKey>>();
		for (RecordKey key : pushKeys) {
			Set<RecordKey> keys = keysPerTables.get(key.getTableName());
			if (keys == null) {
				keys = new HashSet<RecordKey>();
				keysPerTables.put(key.getTableName(), keys);
			}
			keys.add(key);
		}

		// Batch read the records per table
		for (Map.Entry<String, Set<RecordKey>> entry : keysPerTables.entrySet()) {
			Map<RecordKey, CachedRecord> recordMap = VanillaCoreCrud.batchRead(entry.getValue(), tx);

			for (RecordKey key : entry.getValue()) {
				// System.out.println(key);
				CachedRecord rec = recordMap.get(key);

				// Prevent null pointer exceptions in the destination node
				if (rec == null) {
					rec = new CachedRecord();
					rec.setSrcTxNum(tx.getTransactionNumber());
					rec.setVal("exists", FALSE);
				} else
					rec.setVal("exists", TRUE);

				ts.addTuple(key, tx.getTransactionNumber(), tx.getTransactionNumber(), rec);
			}
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Stop-and-copy pushes " + ts.size()
					+ " records to the dest. node. (Node." + targetNode + ")");

		// Push to the destination
		Elasql.connectionMgr().pushTupleSet(targetNode, ts);
	}
	
	private void waitForAck(Transaction tx, CalvinCacheMgr cacheMgr, int targetNode) {
		RecordKey key = NotificationPartitionPlan.createRecordKey(
				targetNode, Elasql.serverId());
		CachedRecord rec = cacheMgr.readFromRemote(key);
		if (rec.getSrcTxNum() != tx.getTransactionNumber() || rec == null)
			throw new RuntimeException("something wrong with the ack: " + key);
	}
	
	private void receiveAndInsertDataChunk(Transaction tx, CalvinCacheMgr cacheMgr, Set<RecordKey> keys) {
		if (logger.isLoggable(Level.INFO))
			logger.info("Stop-and-copy is receiving " + keys.size()
					+ " records from the source node.");

		// Receive the data from the source node and save them
		for (RecordKey k : keys) {
			CachedRecord rec = cacheMgr.readFromRemote(k);
			
			if (rec == null)
				throw new RuntimeException("Something wrong: " + k);

			// Flush them to the local storage engine
			if (rec.getVal("exists").equals(TRUE)) {
				rec.getFldValMap().remove("exists");
				rec.getDirtyFldNames().remove("exists");
				
				cacheMgr.insert(k, rec.getFldValMap());
			}
		}
		
		cacheMgr.flush();
		cacheMgr.clearCachedRecords();
	}
	
	private void sendAnAck(Transaction tx, int targetNode) {
		// Construct pushing tuple set
		TupleSet ts = new TupleSet(-1);
		RecordKey key = NotificationPartitionPlan.createRecordKey(
				Elasql.serverId(), targetNode);
		CachedRecord dummyRec = NotificationPartitionPlan.createRecord(
				Elasql.serverId(), targetNode, tx.getTransactionNumber());
		ts.addTuple(key, tx.getTransactionNumber(), tx.getTransactionNumber(), dummyRec);
		
		// Push to the target
		Elasql.connectionMgr().pushTupleSet(targetNode, ts);
	}
	
	public void updateMigrationRange(MigrationRangeUpdate update) {
		// do nothing
	}
	
	public void sendRangeFinishNotification() {
		// do nothing
	}
	
	public void finishMigration(Transaction tx, Object[] params) {
		// do nothing
	}
	
	public boolean isMigratingRecord(RecordKey key) {
		return false;
	}
	
	public boolean isMigrated(RecordKey key) {
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public void setMigrated(RecordKey key) {
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public int checkSourceNode(RecordKey key) {
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public int checkDestNode(RecordKey key) {
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public boolean isInMigration() {
		return false;
	}
	
	public ReadWriteSetAnalyzer newAnalyzer() {
		throw new RuntimeException("there is no dedicated analyzer for stop-copy");
	}
}
