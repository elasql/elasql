package org.elasql.cache.tpart;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.RemoteRecordReceiver;
import org.elasql.cache.VanillaCoreCrud;
import org.elasql.remote.groupcomm.Tuple;
import org.elasql.sql.RecordKey;
import org.elasql.storage.metadata.PartitionMetaMgr;
import org.elasql.storage.tx.concurrency.tpart.LocalStorageCcMgr;
import org.vanilladb.core.storage.tx.Transaction;

public class TPartCacheMgr implements RemoteRecordReceiver {

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

	private static LocalStorageCcMgr localCcMgr = new LocalStorageCcMgr();

	private Map<CachedEntryKey, CachedRecord> exchange;
	
	private Map<RecordKey, CachedRecord> recordCache;

	private final Object anchors[] = new Object[1009];

	public TPartCacheMgr() {
		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}
		
		if (PartitionMetaMgr.LOC_TABLE_MAX_SIZE == -1) {
			recordCache = new ConcurrentHashMap<RecordKey, CachedRecord>();
			exchange = new ConcurrentHashMap<CachedEntryKey, CachedRecord>();
		}
			
		else {
			recordCache = new ConcurrentHashMap<RecordKey, CachedRecord>(PartitionMetaMgr.LOC_TABLE_MAX_SIZE + 1000);
			exchange = new ConcurrentHashMap<CachedEntryKey, CachedRecord>(PartitionMetaMgr.LOC_TABLE_MAX_SIZE + 1000);
		}
			
		
//		new PeriodicalJob(3000, 500000, new Runnable() {
//			@Override
//			public void run() {
//				System.out.println("Cache Size : " + recordCache.size());
//			}
//		}).start();
	}

	private Object prepareAnchor(Object o) {
		int hash = o.hashCode() % anchors.length;
		if (hash < 0) {
			hash += anchors.length;
		}
		return anchors[0];
	}

	CachedRecord takeFromTx(RecordKey key, long src, long dest) {
		CachedEntryKey k = new CachedEntryKey(key, src, dest);
		synchronized (prepareAnchor(k)) {
			try {
				// wait if the record has not delivered
				while (!exchange.containsKey(k)) {
					prepareAnchor(k).wait();
				}
				return exchange.remove(k);
			} catch (InterruptedException e) {
				throw new RuntimeException();
			}
		}
	}

	void passToTheNextTx(RecordKey key, CachedRecord rec, long src, long dest, boolean isRemote) {
		if (rec == null)
			throw new NullPointerException(String.format(
					"The record for %s is null (from Tx.%d to Tx.%d)", key, src, dest));
		
		CachedEntryKey k = new CachedEntryKey(key, src, dest);
		synchronized (prepareAnchor(k)) {
			exchange.put(k, rec);
			prepareAnchor(k).notifyAll();
		}
	}

	@Override
	public void cacheRemoteRecord(Tuple t) {
		passToTheNextTx(t.key, t.rec, t.srcTxNum, t.destTxNum, true);
	}
	
	CachedRecord readFromSink(RecordKey key, Transaction tx) {
		localCcMgr.beforeSinkRead(key, tx.getTransactionNumber());
		
		CachedRecord rec = null;
		
		// Check the cache first
		rec = recordCache.get(key);
		if (rec != null) // Copy the record to ensure thread-safety
			rec = new CachedRecord(rec);
		
		// Read from the local storage
		if (rec == null)
			rec = VanillaCoreCrud.read(key, tx);
		
		localCcMgr.afterSinkRead(key, tx.getTransactionNumber());
		
		if (rec == null)
			throw new RuntimeException("Cannot find the record of " + key);
		
		return rec;
	}
	
	void insertToCache(RecordKey key, CachedRecord rec, long txNum) {
		localCcMgr.beforeWriteBack(key, txNum);
		
		recordCache.put(key, rec);
		
		localCcMgr.afterWriteback(key, txNum);
	}
	
	void deleteFromCache(RecordKey key, long txNum) {
		localCcMgr.beforeWriteBack(key, txNum);
		
		if (recordCache.remove(key) == null)
			throw new RuntimeException("There is no record for " + key + " in the cache");
		
		localCcMgr.afterWriteback(key, txNum);
	}
	
	void writeBack(RecordKey key, CachedRecord rec, Transaction tx) {
		localCcMgr.beforeWriteBack(key, tx.getTransactionNumber());
		
		// Check if there is corresponding keys in the cache
		if (recordCache.containsKey(key))
			recordCache.put(key, rec);
		
		// If it should not be in the cache, write-back to the local storage
		writeToVanillaCore(key, rec, tx);
		
		localCcMgr.afterWriteback(key, tx.getTransactionNumber());
	}
	
	public void registerSinkReading(RecordKey key, long txNum) {
		localCcMgr.requestSinkRead(key, txNum);
	}

	public void registerSinkWriteback(RecordKey key, long txNum) {
		localCcMgr.requestWriteBack(key, txNum);
	}
	
	private void writeToVanillaCore(RecordKey key, CachedRecord rec, Transaction tx) {
		if (rec.isDeleted())
			VanillaCoreCrud.delete(key, tx);
		else if (rec.isNewInserted())
			VanillaCoreCrud.insert(key, rec, tx);
		else if (rec.isDirty())
			VanillaCoreCrud.update(key, rec, tx);
	}
}
