package org.elasql.cache.tpart;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.elasql.cache.CachedRecord;
import org.elasql.schedule.tpart.sink.SunkPlan;
import org.elasql.server.Elasql;
import org.elasql.sql.PrimaryKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.tx.Transaction;

public class TPartTxLocalCache {

	private Transaction tx;
	private long txNum;
	private TPartCacheMgr cacheMgr;
	private Map<PrimaryKey, CachedRecord> recordCache = new HashMap<PrimaryKey, CachedRecord>();
	private long localStorageId;

	public TPartTxLocalCache(Transaction tx) {
		this.tx = tx;
		this.txNum = tx.getTransactionNumber();
		this.cacheMgr = (TPartCacheMgr) Elasql.remoteRecReceiver();
		this.localStorageId = TPartCacheMgr.toSinkId(Elasql.serverId());
	}

	/**
	 * Reads a CachedRecord with the specified key from a previous sink. A sink
	 * may be a T-Graph or the local storage.
	 * 
	 * @param key
	 *            the key of the record
	 * @param mySinkId
	 *            the id of the sink where the transaction executes
	 * @return the specified record
	 */
	public CachedRecord readFromSink(PrimaryKey key) {

		CachedRecord rec = null;
		rec = cacheMgr.readFromSink(key, tx);
		rec.setSrcTxNum(txNum);
		recordCache.put(key, rec);

		return rec;
	}
	
	public Map<PrimaryKey, CachedRecord> batchReadFromSink(Set<PrimaryKey> keys) {
		Map<PrimaryKey, CachedRecord> records = cacheMgr.batchReadFromSink(keys, tx);
		
		for (CachedRecord rec : records.values()) {
			rec.setSrcTxNum(txNum);
		}
		// XXX: Comment this to speed up
		// recordCache.putAll(records);
		
		return records;
	}

	/**
	 * Reads a CachedRecord from the cache. If the specified record does not
	 * exist, reads from the specified transaction through {@code TPartCacheMgr}
	 * .
	 * 
	 * @param key
	 *            the key of the record
	 * @param src
	 *            the id of the transaction who will pass the record to the
	 *            caller
	 * @return the specified record
	 */
	public CachedRecord read(PrimaryKey key, long src) {
		
		CachedRecord rec = recordCache.get(key);
		if (rec != null)
			return rec;

		rec = cacheMgr.takeFromTx(key, src, txNum);
		recordCache.put(key, rec);
		
		return rec;
	}

	public void update(PrimaryKey key, CachedRecord rec) {
		rec.setSrcTxNum(txNum);
		recordCache.put(key, rec);
	}

	public void insert(PrimaryKey key, Map<String, Constant> fldVals) {
		CachedRecord rec = CachedRecord.newRecordForInsertion(key, fldVals);
		rec.setSrcTxNum(tx.getTransactionNumber());
		recordCache.put(key, rec);
	}

	public void delete(PrimaryKey key) {
		CachedRecord dummyRec = CachedRecord.newRecordForDeletion(key);
		dummyRec.setSrcTxNum(tx.getTransactionNumber());
		recordCache.put(key, dummyRec);
	}

	public void flush(SunkPlan plan, List<CachedEntryKey> cachedEntrySet) {
//		Timer timer = Timer.getLocalTimer();
		
		// Pass to the transactions
//		timer.startComponentTimer("Pass to next Tx");
		for (Map.Entry<PrimaryKey, CachedRecord> entry : recordCache.entrySet()) {
			Long[] dests = plan.getLocalPassingTarget(entry.getKey());
			if (dests != null) {
				for (long dest : dests) {
					// The destination might be the local storage (txNum < 0)
					if (dest >= 0) {
						CachedRecord clonedRec = new CachedRecord(entry.getValue());
						cacheMgr.passToTheNextTx(entry.getKey(), clonedRec, txNum, dest, false);
					}
				}
			}
		}
//		timer.stopComponentTimer("Pass to next Tx");

//		timer.startComponentTimer("Writeback");
		// Flush to the local storage (write back)
		for (PrimaryKey key : plan.getLocalWriteBackInfo()) {

			CachedRecord rec = null;
			if (plan.isHereMaster()) 
				rec = recordCache.get(key);
			else
				rec = cacheMgr.takeFromTx(key, txNum, localStorageId);
			
			// For migration
			if (plan.getStorageInsertions().contains(key)) {
				cacheMgr.insertToLocalStorage(key, rec, tx);
				continue;
			}

			// If there is no such record in the local cache,
			// it might be pushed from the same transaction on the other
			// machine.
			// Migrated data need to insert
			if (plan.getCacheInsertions().contains(key))
				cacheMgr.insertToCache(key, rec, txNum);
			else
				cacheMgr.writeBack(key, rec, tx);
		}
//		timer.stopComponentTimer("Writeback");
		
		// Clean up migrated rec
//		timer.startComponentTimer("Delete cached records");
		for (PrimaryKey key : plan.getCacheDeletions())
			cacheMgr.deleteFromCache(key, txNum);
//		timer.stopComponentTimer("Delete cached records");
	}
}
