package org.elasql.migration.zephyr;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.VanillaCoreCrud;
import org.elasql.cache.calvin.CalvinCacheMgr;
import org.elasql.cache.calvin.CalvinPostOffice;
import org.elasql.migration.MigrationComponentFactory;
import org.elasql.migration.MigrationMgr;
import org.elasql.migration.MigrationRange;
import org.elasql.migration.MigrationRangeFinishMessage;
import org.elasql.migration.MigrationRangeUpdate;
import org.elasql.migration.MigrationSystemController;
import org.elasql.remote.groupcomm.ByteSet;
import org.elasql.remote.groupcomm.StoredProcedureCall;
import org.elasql.remote.groupcomm.TupleSet;
import org.elasql.schedule.calvin.CalvinScheduler;
import org.elasql.schedule.calvin.ReadWriteSetAnalyzer;
import org.elasql.schedule.calvin.Zephyr.ZephyrAnalyzer;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.NotificationPartitionPlan;
import org.elasql.storage.metadata.PartitionPlan;
import org.vanilladb.core.server.VanillaDb;
import org.vanilladb.core.storage.index.Index;
import org.vanilladb.core.storage.index.SearchKeyType;
import org.vanilladb.core.storage.metadata.index.IndexInfo;
import org.vanilladb.core.storage.tx.Transaction;

public class ZephyrMigrationMgr implements MigrationMgr {
	private static final int BYTE_BUFFER_SIZE = 1048576;

	private static Logger logger = Logger.getLogger(ZephyrMigrationMgr.class.getName());
	
	private List<MigrationRange> migrationRanges;
	private List<MigrationRange> pushRanges = new ArrayList<MigrationRange>(); // the ranges whose destination is this node.
	private PartitionPlan newPartitionPlan;
	private MigrationComponentFactory comsFactory;
	private boolean isInMigration;
	private List<IndexInfo> allIndexes = new LinkedList<IndexInfo>();
	private List<SearchKeyType> allSKtype = new LinkedList<SearchKeyType>();
	private boolean dualPhase;
	private Set<Integer> allSourceSet = new HashSet<Integer>();
	private Set<Integer> allDestSet = new HashSet<Integer>();
	Transaction class_tx;
	
	public ZephyrMigrationMgr(MigrationComponentFactory comsFactory) {
		this.comsFactory = comsFactory;
	}
	
	public void initializeMigration(Transaction tx, Object[] params) {
		
		// Parse parameters
		PartitionPlan newPartPlan = (PartitionPlan) params[0];
		
		if (logger.isLoggable(Level.INFO)) {
			long time = System.currentTimeMillis() - CalvinScheduler.FIRST_TX_ARRIVAL_TIME.get();
			PartitionPlan currentPartPlan = Elasql.partitionMetaMgr().getPartitionPlan();
			logger.info(String.format("a new migration starts at %d. Current Plan: %s, New Plan: %s"
					, time / 1000, currentPartPlan, newPartPlan));
		}
		
		// Initialize states
		//tx need check
		class_tx = tx;
		//create a set of dest for copying index file
		Set<Integer> indexdest = new HashSet<Integer>();
//		isInMigration = true;
//		dualPhase = false;
		PartitionPlan currentPlan = Elasql.partitionMetaMgr().getPartitionPlan();
		if (currentPlan.getClass().equals(NotificationPartitionPlan.class))
			currentPlan = ((NotificationPartitionPlan) currentPlan).getUnderlayerPlan();
		migrationRanges = comsFactory.generateMigrationRanges(currentPlan, newPartPlan);
		for (MigrationRange range : migrationRanges) {
			if (range.getDestPartId() == Elasql.serverId())
				pushRanges.add(range);
			else if(range.getSourcePartId() == Elasql.serverId())
				indexdest.add(range.getDestPartId());
			
			allSourceSet.add(range.getSourcePartId());
			allDestSet.add(range.getDestPartId());
		}
		newPartitionPlan = newPartPlan;
		
		if (logger.isLoggable(Level.INFO)) {
			logger.info(String.format("migration ranges: %s", migrationRanges.toString()));
		}
		isInMigration = true;
		dualPhase = false;
		
		if(allSourceSet.contains(Elasql.serverId()) || allDestSet.contains(Elasql.serverId()))
			waitForInsertTransactionFinish();
		//
		//move index to dest
		if (logger.isLoggable(Level.INFO)) {
			logger.info(String.format("start to move index"));
		}
		
		for(Integer id:indexdest) {
			CopyIndex(id, tx);
		}
		
//		if(Elasql.serverId() == sourcenodeId) {
//			CopyIndex(migrationRanges.get(0).getDestPartId(), tx);
//		}
		
		if (logger.isLoggable(Level.INFO)) {
			logger.info(String.format("move index is finish"));
		}
		//finish move index to dest
		//
//		if (!pushRanges.isEmpty())
//			scheduleNextBGPushRequest();
//		
//		if (logger.isLoggable(Level.INFO)) {
//			logger.info(String.format("migration ranges: %s", migrationRanges.toString()));
//		}
	}
	public void waitForInsertTransactionFinish() {
		while (VanillaDb.txMgr().getInsertTxCount() > 1) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			if (logger.isLoggable(Level.INFO)) {
				logger.info(String.format("current insert transaction count: %d",
						VanillaDb.txMgr().getInsertTxCount()));
			}
		}
	}
	
	
	private void CopyIndex(int nodeId, Transaction tx) {
		ByteSet fs = new ByteSet();
		FileInputStream fip = null;
		List<File> IndexList = VanillaDb.fileMgr().FindAllIndex();
		int count = 0;
		
//		for(File f : IndexList) {
//			if (logger.isLoggable(Level.INFO))
//				logger.info("file name: " + f.getName());
//		}
		
		fs.addByte("startMigrate", IndexList.size(), IndexList.size(), null,
				IndexList.size(),null,null);
		Elasql.connectionMgr().pushByteSet(nodeId, fs);
		fs.clear();
		
		//List<String> IndexListName = new ArrayList<String>();
		for(File f : IndexList) {
			String name = f.getName();
			if (logger.isLoggable(Level.INFO))
				logger.info("file name: " + f.getName());
			if (name.contains("_leaf.idx") || name.contains("_dir.idx")) {
				name=name.replaceAll("_leaf.idx", "");
		        name=name.replaceAll("_dir.idx", "");
		    }
			IndexInfo ii =  VanillaDb.catalogMgr().getIndexInfoByName(name, tx);
			if(ii == null) {
				if (logger.isLoggable(Level.INFO))
					logger.info("wrong on " + f.getName());
			}
			IndexInfo ii_trans = new IndexInfo("tmp_"+ii.indexName(), ii.tableName(),
					ii.fieldNames(), ii.indexType());
			SearchKeyType sktype = new SearchKeyType(VanillaDb.catalogMgr().
					getTableInfo(ii.tableName(), tx).schema(), ii.fieldNames());
			allIndexes.add(ii);
			allSKtype.add(sktype);
			try {
				//IndexListName.add(f.getName());
				fip = new FileInputStream(f);
				String serial_nm = "tmp_" + f.getName();
				int byte_lenth = 0;
				int part = 0;
				//int total_part = (fip.available()/BYTE_BUFFER_SIZE) + 1;
				int total_part = (int)((f.length()/BYTE_BUFFER_SIZE));
				if(f.length() % BYTE_BUFFER_SIZE != 0)
					total_part += 1;
				while (true) {
					byte[] buf = new byte[BYTE_BUFFER_SIZE];
					if((byte_lenth = fip.read(buf)) == -1)
						break;
					fs.addByte(serial_nm, part, total_part, buf, byte_lenth,ii_trans,sktype);
					part++;
					count++;
					if(count >= 10) {
						if (logger.isLoggable(Level.INFO))
							logger.info("start to move index " + fs.size()
									+ " Bytes to the dest. node. (Node." + nodeId + ")");
						Elasql.connectionMgr().pushByteSet(nodeId, fs);
						fs.clear();
						count = 0;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}finally{
				try {
					if(fip != null) {
						fip.close();
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
		if (logger.isLoggable(Level.INFO))
			logger.info("start to move index " + fs.size()
					+ " Bytes to the dest. node. (Node." + nodeId + ")");
		
		Elasql.connectionMgr().pushByteSet(nodeId, fs);
	}
	
	public void scheduleNextBGPushRequest() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				for (MigrationRange range : pushRanges) {
					Set<RecordKey> chunk = range.generateNextMigrationChunk(USE_BYTES_FOR_CHUNK_SIZE, CHUNK_SIZE);
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
	
	public void sendBGPushRequest(MigrationRangeUpdate update, Set<RecordKey> chunk,
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
		for (RecordKey key : chunk)
			params[i++] = key;
		
		// Send a store procedure call
		Object[] call = { new StoredProcedureCall(-1, -1, 
				ZephyrStoredProcFactory.SP_BG_PUSH, params)};
		Elasql.connectionMgr().sendBroadcastRequest(call, false);
	}
	
	public void addNewInsertKeyOnSource(RecordKey key) {
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
		dualPhase = false;
		migrationRanges.clear();
		pushRanges.clear();
	}
	
	public boolean isMigratingRecord(RecordKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return true;
		return false;
	}
	
	public boolean isMigrated(RecordKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key)) {
				boolean torf = VanillaCoreCrud.isMigrated(key, class_tx, allIndexes, allSKtype);
//				if(torf != range.isMigrated(key)) {
//					if (logger.isLoggable(Level.INFO)) {
//						logger.info("/n/n/nisMigrated have problem/n/n/n");
//					}
//				}
//				else {
//					if (logger.isLoggable(Level.INFO)) {
//						logger.info("/n/n/nisMigrated/n/n/n");
//					}
//				}
				
				//return range.isMigrated(key);
				return torf;
			}
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public void setMigrated(RecordKey key) {
		int flag = 0;
		for (MigrationRange range : migrationRanges)
			if (range.contains(key)) {
				flag = VanillaCoreCrud.setMigrated(key, class_tx, allIndexes, allSKtype);
//				if (logger.isLoggable(Level.INFO)) {
//					logger.info(String.format("flag: %d.", flag));
//				}
				//range.setMigrated(key);
				return;
			}
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public int checkSourceNode(RecordKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return range.getSourcePartId();
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public int checkDestNode(RecordKey key) {
		for (MigrationRange range : migrationRanges)
			if (range.contains(key))
				return range.getDestPartId();
		throw new RuntimeException(String.format("%s is not a migrating record", key));
	}
	
	public boolean isInMigration() {
		return isInMigration;
	}
	
	public ReadWriteSetAnalyzer newAnalyzer() {
		return new ZephyrAnalyzer();
	}

	public void changePhase(Transaction tx, Object[] params) {
		if (logger.isLoggable(Level.INFO)) {
			logger.info(String.format("ready get indexinfo and type"));
		}
		if(Elasql.serverId() == migrationRanges.get(0).getDestPartId()) {
			allIndexes = Elasql.CallAcceptByteSave().getIndexInfo();
			allSKtype = Elasql.CallAcceptByteSave().getSearchKeyType();
		}
		dualPhase = true;
		
		if (!pushRanges.isEmpty())
			scheduleNextBGPushRequest();
		
		if (logger.isLoggable(Level.INFO)) {
			logger.info(String.format("migration ranges: %s", migrationRanges.toString()));
		}
	}
	public boolean isInDual() {
		return dualPhase;
	}
	
	public Set<Integer> getSourceId() {
		return allSourceSet;
	}
	
	public Set<Integer> getDestId() {
		return allDestSet;
	}
}