package org.elasql.cache.tpart;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.RemoteRecordReceiver;
import org.elasql.cache.VanillaCoreCrud;
import org.elasql.remote.groupcomm.Tuple;
import org.elasql.schedule.tpart.hermes.FusionTable;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.TransactionProfiler;

public class TPartCacheMgr implements RemoteRecordReceiver {
	private static Logger logger = Logger.getLogger(TPartCacheMgr.class.getName());

	/**
	 * Looks up the sink id for the specified partition.
	 * 
	 * @param partId
	 *            the id of the specified partition
	 * @return the sink id
	 */
	public static long toSinkId(int partId) {
		return (partId + 1) * -1;
	}

//	private static LocalStorageCcMgr localCcMgr = new LocalStorageCcMgr();
//	private static LocalStorageLockTable lockTable = new LocalStorageLockTable();

	private Map<CachedEntryKey, EnhancedCachedRecord> exchange;
	
	private Map<PrimaryKey, CachedRecord> recordCache;

	private final Object anchors[] = new Object[1009];

	public TPartCacheMgr() {
		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}
		
		recordCache = new ConcurrentHashMap<PrimaryKey, CachedRecord>(FusionTable.EXPECTED_MAX_SIZE + 1000);
		exchange = new ConcurrentHashMap<CachedEntryKey, EnhancedCachedRecord>(FusionTable.EXPECTED_MAX_SIZE + 1000);
		
//		new PeriodicalJob(5000, 600000, new Runnable() {
//			@Override
//			public void run() {
//				long time = System.currentTimeMillis() - Elasql.START_TIME_MS;
//				time /= 1000;
//				System.out.println(String.format("Time: %d seconds, Cache Size: %d, Exchange Size: %d",
//						time, recordCache.size(), exchange.size()));
//			}
//		}).start();
		
//		new PeriodicalJob(60_000, 800_000, new Runnable() {
//			@Override
//			public void run() {
//				long time = System.currentTimeMillis() - Elasql.START_TIME_MS;
//				time /= 1000;
//				System.out.println(String.format("Time: %d seconds, suggest to GC.", time));
//				System.gc();
//			}
//		}).start();
	}

	private Object prepareAnchor(Object o) {
		int hash = o.hashCode() % anchors.length;
		if (hash < 0) {
			hash += anchors.length;
		}
		return anchors[hash];
	}

	CachedRecord takeFromTx(PrimaryKey key, long src, long dest) {
//		Timer.getLocalTimer().startComponentTimer("Read from Tx");
//		try {
			CachedEntryKey k = new CachedEntryKey(key, src, dest);
			synchronized (prepareAnchor(k)) {
				try {
					// Debug: Tracing the waiting key
//					Thread.currentThread().setName("Tx." + dest + " waits for pushing of " + key
//							+ " from tx." + src);
					// wait if the record has not delivered
					while (!exchange.containsKey(k)) {
						prepareAnchor(k).wait();
					}
					
					EnhancedCachedRecord r = exchange.remove(k);
					CachedRecord rec = r.getCachedRecord();
					
					// For controller
					if (r.isRemote())
						TransactionProfiler.getLocalProfiler().incrementNetworkInSize(rec);
					
					// Debug: Tracing the waiting key
//					Thread.currentThread().setName("Tx." + dest);
					
					return rec;
				} catch (InterruptedException e) {
					throw new RuntimeException();
				}
			}
//		} finally {
//			Timer.getLocalTimer().stopComponentTimer("Read from Tx");
//		}
	}

	void passToTheNextTx(PrimaryKey key, CachedRecord rec, long src, long dest, boolean isRemote) {
		if (rec == null)
			throw new NullPointerException(String.format(
					"The record for %s is null (from Tx.%d to Tx.%d)", key, src, dest));
		
		CachedEntryKey k = new CachedEntryKey(key, src, dest);
		EnhancedCachedRecord r = new EnhancedCachedRecord(rec, isRemote);
		
		synchronized (prepareAnchor(k)) {
			exchange.put(k, r);
			prepareAnchor(k).notifyAll();
		}
	}

	@Override
	public void cacheRemoteRecord(Tuple t) {
		passToTheNextTx(t.key, t.rec, t.srcTxNum, t.destTxNum, true);
	}
	
	CachedRecord readFromSink(PrimaryKey key, Transaction tx) {
//		localCcMgr.beforeSinkRead(key, tx.getTransactionNumber());
//		lockTable.sLock(key, tx.getTransactionNumber());
		
		CachedRecord rec = null;
		
		// Check the cache first
		rec = recordCache.get(key);
		if (rec != null) // Copy the record to ensure thread-safety
			rec = new CachedRecord(rec);
		
		// Read from the local storage
		if (rec == null)
			rec = VanillaCoreCrud.read(key, tx);
		
//		localCcMgr.afterSinkRead(key, tx.getTransactionNumber());
//		lockTable.release(key, tx.getTransactionNumber(), LockType.S_LOCK);
		
		if (rec == null)
			throw new RuntimeException("Tx." + tx.getTransactionNumber()
				+ " cannot find the record of " + key);
		
		return rec;
	}
	
	Map<PrimaryKey, CachedRecord> batchReadFromSink(Set<PrimaryKey> keys, Transaction tx) {
//		for (RecordKey key : keys)
//			lockTable.sLock(key, tx.getTransactionNumber());
		
		Map<PrimaryKey, CachedRecord> records = new HashMap<PrimaryKey, CachedRecord>();
		
		// Check the cache first
		Set<PrimaryKey> readFromLocals = new HashSet<PrimaryKey>();
		for (PrimaryKey key : keys) {
			CachedRecord rec = recordCache.get(key);
			if (rec != null) {
				// Copy the record to ensure thread-safety
				rec = new CachedRecord(rec);
				records.put(key, rec);
			} else {
				readFromLocals.add(key);
			}
		}
		
		// Read from the local storage
		if (!readFromLocals.isEmpty()) {
			Map<PrimaryKey, CachedRecord> localReads =
					VanillaCoreCrud.batchRead(readFromLocals, tx);
			records.putAll(localReads);
		}
		
//		for (RecordKey key : keys)
//			lockTable.release(key, tx.getTransactionNumber(), LockType.S_LOCK);
		
		return records;
	}
	
	void insertToCache(PrimaryKey key, CachedRecord rec, long txNum) {
//		localCcMgr.beforeWriteBack(key, txNum);
//		lockTable.xLock(key, txNum);

		recordCache.put(key, rec);
		
//		localCcMgr.afterWriteback(key, txNum);
//		lockTable.release(key, txNum, LockType.X_LOCK);
	}
	
	void deleteFromCache(PrimaryKey key, long txNum) {
//		localCcMgr.beforeWriteBack(key, txNum);
//		lockTable.xLock(key, txNum);

		if (recordCache.remove(key) == null)
			throw new RuntimeException("There is no record for " + key + " in the cache");
		
//		localCcMgr.afterWriteback(key, txNum);
//		lockTable.release(key, txNum, LockType.X_LOCK);
	}
	
	void writeBack(PrimaryKey key, CachedRecord rec, Transaction tx) {
//		localCcMgr.beforeWriteBack(key, tx.getTransactionNumber());
//		lockTable.xLock(key, tx.getTransactionNumber());
		
		// Check if there is corresponding keys in the cache
		if (recordCache.containsKey(key))
			recordCache.put(key, rec);
		else 
			// If it was not in the cache, write-back to the local storage
			writeToVanillaCore(key, rec, tx);
		
//		localCcMgr.afterWriteback(key, tx.getTransactionNumber());
//		lockTable.release(key, tx.getTransactionNumber(), LockType.X_LOCK);
	}
	
	// This is also a type of writeback
	void insertToLocalStorage(PrimaryKey key, CachedRecord rec, Transaction tx) {
//		localCcMgr.beforeWriteBack(key, tx.getTransactionNumber());
//		lockTable.xLock(key, tx.getTransactionNumber());
		
		// Check if there is corresponding keys in the cache
		if (recordCache.containsKey(key))
			recordCache.remove(key);
		
		// Force insert to local storage
		rec.setNewInserted();
		VanillaCoreCrud.insert(key, rec, tx);
		
//		localCcMgr.afterWriteback(key, tx.getTransactionNumber());
//		lockTable.release(key, tx.getTransactionNumber(), LockType.X_LOCK);
	}
	
//	public void registerSinkReading(RecordKey key, long txNum) {
//		localCcMgr.requestSinkRead(key, txNum);
//		lockTable.requestLock(key, txNum);
//	}

//	public void registerSinkWriteback(RecordKey key, long txNum) {
//		localCcMgr.requestWriteBack(key, txNum);
//		lockTable.requestLock(key, txNum);
//	}
	
	private void writeToVanillaCore(PrimaryKey key, CachedRecord rec, Transaction tx) {
		if (rec.isDeleted())
			VanillaCoreCrud.delete(key, tx);
		else if (rec.isNewInserted())
			VanillaCoreCrud.insert(key, rec, tx);
		else if (rec.isDirty()) {
			if (!VanillaCoreCrud.update(key, rec, tx)) {
				// XXX: We use this to solve a migration problem
				// If a hot record was on other machine and belonged to another partition (not local one),
				// then the partitioning changed, the hot record would not go to the new partition immediately.
				// It is highly possible the record would be written back to the new partition very soon.
				// However, we didn't make hot records be inserted to the new partition.
				// Therefore, they would not be in the database.
				// In this case, we should insert the record if we could not find it.
				if (logger.isLoggable(Level.FINE))
					logger.fine("Insert the record " + key + " since we could not find it.");
				
				VanillaCoreCrud.insert(key, rec, tx);
			}
		}
	}
}
