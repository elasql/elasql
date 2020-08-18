package org.elasql.migration.albatross;

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
import org.elasql.migration.MigrationPlan;
import org.elasql.migration.MigrationRange;
import org.elasql.migration.MigrationRangeFinishMessage;
import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.migration.MigrationSettings;
import org.elasql.migration.MigrationSystemController;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.schedule.calvin.CalvinScheduler;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.schedule.calvin.albatross.AlbatrossAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.storage.tx.Transaction;

/**
 * Main Idea: executes the transactions on the source node, migrates records using bg pushes, records
 * the changes during the migration, and uses an atomic handover to move the changes to the destination.
 */
public class AlbatrossMigrationMgr implements MigrationMgr {
	private static Logger logger = Logger.getLogger(AlbatrossMigrationMgr.class.getName());
	
	private List<MigrationRange> migrationRanges;
	private List<MigrationRange> pushRanges = new ArrayList<MigrationRange>(); // the ranges whose destination is this node.
	private PartitionPlan newPartitionPlan;
	private boolean isInMigration;
	private int localNodeId;
	
	// <Source || Destination> -> <Key Set>
	// XXX: Assume there is no cycle that a node is both the source and destination for another node.
	private Map<Integer, Set<PrimaryKey>> updatedKeys = new HashMap<Integer, Set<PrimaryKey>>();
	private Map<Integer, Set<PrimaryKey>> insertedKeys = new HashMap<Integer, Set<PrimaryKey>>();
	private Map<Integer, Set<PrimaryKey>> deletedKeys = new HashMap<Integer, Set<PrimaryKey>>();
	
	private MigrationComponentFactory comsFactory;
	
	public AlbatrossMigrationMgr(MigrationComponentFactory comsFactory) {
		this.localNodeId = Elasql.serverId();
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
			if (range.getDestPartId() == localNodeId)
				pushRanges.add(range);
		newPartitionPlan = newPartPlan;
		
		// Initialize change sets
		for (MigrationRange range : migrationRanges) {
			if (range.getSourcePartId() == localNodeId) {
				updatedKeys.put(range.getDestPartId(), new HashSet<PrimaryKey>());
				insertedKeys.put(range.getDestPartId(), new HashSet<PrimaryKey>());
				deletedKeys.put(range.getDestPartId(), new HashSet<PrimaryKey>());
			}
			if (range.getDestPartId() == localNodeId) {
				updatedKeys.put(range.getSourcePartId(), new HashSet<PrimaryKey>());
				insertedKeys.put(range.getSourcePartId(), new HashSet<PrimaryKey>());
				deletedKeys.put(range.getSourcePartId(), new HashSet<PrimaryKey>());
			}
		}
		
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
				AlbatrossStoredProcFactory.SP_BG_PUSH, params);
	}
	
	public void addUpdatedKey(PrimaryKey key) {
		for (MigrationRange range : migrationRanges) {
			if (range.contains(key)) {
				if (range.getSourcePartId() == localNodeId)
					updatedKeys.get(range.getDestPartId()).add(key);
				else if (range.getDestPartId() == localNodeId)
					updatedKeys.get(range.getSourcePartId()).add(key);
				return;
			}
		}
	}
	
	public void addInsertedKey(PrimaryKey key) {
		for (MigrationRange range : migrationRanges) {
			if (range.contains(key)) {
				if (range.getSourcePartId() == localNodeId)
					insertedKeys.get(range.getDestPartId()).add(key);
				else if (range.getDestPartId() == localNodeId)
					insertedKeys.get(range.getSourcePartId()).add(key);
				return;
			}
		}
	}
	
	public void addDeletedKey(PrimaryKey key) {
		for (MigrationRange range : migrationRanges) {
			if (range.contains(key)) {
				if (range.getSourcePartId() == localNodeId)
					deletedKeys.get(range.getDestPartId()).add(key);
				else if (range.getDestPartId() == localNodeId)
					deletedKeys.get(range.getSourcePartId()).add(key);
				return;
			}
		}
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
		
		// Atomic handover
		performAtomicHandover(tx);
				
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
		return new AlbatrossAnalyzer();
	}
	
	private void performAtomicHandover(Transaction tx) {
		if (logger.isLoggable(Level.INFO)) {
			long time = System.currentTimeMillis() - CalvinScheduler.FIRST_TX_ARRIVAL_TIME.get();
			logger.info(String.format("Atomic handover starts at %d."
					, time / 1000));
		}
		
		// Perform as the source node, read and push all the changed records
		for (MigrationRange range : migrationRanges) {
			if (range.getSourcePartId() == localNodeId) {
				readAndPushChanges(tx, range.getDestPartId());
			}
		}

		// create a CacheMgr
		CalvinPostOffice postOffice = (CalvinPostOffice) Elasql.remoteRecReceiver();
		CalvinCacheMgr cacheMgr = postOffice.createCacheMgr(tx, true);
		
		// Perform as the dest node, receive and update all the changes
		for (MigrationRange range : migrationRanges) {
			if (range.getDestPartId() == localNodeId) {
				recieveAndUpdateChanges(tx, cacheMgr, range.getSourcePartId());
			}
		}
	}
	
	private Map<PrimaryKey, CachedRecord> readRecords(Transaction tx, Set<PrimaryKey> readKeys) {
		Map<PrimaryKey, CachedRecord> recordMap = new HashMap<PrimaryKey, CachedRecord>();
		
		// Construct key sets
		Map<String, Set<PrimaryKey>> keysPerTables = new HashMap<String, Set<PrimaryKey>>();
		for (PrimaryKey key : readKeys) {
			Set<PrimaryKey> keys = keysPerTables.get(key.getTableName());
			if (keys == null) {
				keys = new HashSet<PrimaryKey>();
				keysPerTables.put(key.getTableName(), keys);
			}
			keys.add(key);
		}

		// Batch read the records per table
		for (Map.Entry<String, Set<PrimaryKey>> entry : keysPerTables.entrySet())
			recordMap.putAll(VanillaCoreCrud.batchRead(entry.getValue(), tx));
		
		return recordMap;
	}
	
	private void readAndPushChanges(Transaction tx, int destination) {
		// Construct pushing tuple set
		TupleSet ts = new TupleSet(-1);
		long txNum = tx.getTransactionNumber();
				
		// Prepare insert set
		Set<PrimaryKey> insertSet = insertedKeys.get(destination);
		Map<PrimaryKey, CachedRecord> records = readRecords(tx, insertSet);
		for (Map.Entry<PrimaryKey, CachedRecord> entry : records.entrySet()) {
			CachedRecord rec = entry.getValue();
			rec.setNewInserted();
			ts.addTuple(entry.getKey(), txNum, txNum, rec);
		}
		
		// Note: we do not have to do anything to deleted records on the source node
		Set<PrimaryKey> deleteSet = deletedKeys.get(destination);
		deleteSet.removeAll(insertSet);
		
		// Prepare update set (excluding inserts and deletes)
		Set<PrimaryKey> updateSet = updatedKeys.get(destination);
		updateSet.removeAll(insertSet);
		updateSet.removeAll(deleteSet);
		records = readRecords(tx, updateSet);
		for (Map.Entry<PrimaryKey, CachedRecord> entry : records.entrySet()) {
			PrimaryKey key = entry.getKey();
			CachedRecord rec = entry.getValue();
			// We do not record which fields are dirty. So, we simply 
			// make it update all non-key fields
			rec.markAllNonKeyFieldsDirty();
			ts.addTuple(key, txNum, txNum, rec);
		}

		if (logger.isLoggable(Level.INFO))
			logger.info("Albatross pushes " + ts.size()
					+ " records to the dest. node. (Node." + destination + ")");

		// Push to the destination
		Elasql.connectionMgr().pushTupleSet(destination, ts);
	}
	
	private void recieveAndUpdateChanges(Transaction tx, CalvinCacheMgr cacheMgr, int source) {
		// Read inserted set
		Set<PrimaryKey> insertSet = insertedKeys.get(source);
		for (PrimaryKey k : insertSet) {
			CachedRecord rec = cacheMgr.readFromRemote(k);
			
			if (rec == null)
				throw new RuntimeException("Something wrong: " + k);

			// Flush them to the local storage engine
			cacheMgr.insert(k, rec);
		}
		
		// Delete records
		Set<PrimaryKey> deleteSet = deletedKeys.get(source);
		deleteSet.removeAll(insertSet);
		for (PrimaryKey k : deleteSet)
			cacheMgr.delete(k);
		
		// Read updated set
		Set<PrimaryKey> updateSet = updatedKeys.get(source);
		updateSet.removeAll(insertSet);
		updateSet.removeAll(deleteSet);
		for (PrimaryKey k : updateSet) {
			CachedRecord rec = cacheMgr.readFromRemote(k);
			
			if (rec == null)
				throw new RuntimeException("Something wrong: " + k);

			// Flush them to the local storage engine
			cacheMgr.update(k, rec);
		}
		
		cacheMgr.flush();
		cacheMgr.clearCachedRecords();

		if (logger.isLoggable(Level.INFO))
			logger.info(String.format("Albatross inserts %d records, deletes %d records, and updates %d"
					+ " records on the destination node.", insertSet.size(), deleteSet.size(), updateSet.size()));
	}
}
