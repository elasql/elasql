package org.elasql.cache.tpart;

import java.util.HashMap;
import java.util.Map;

import org.elasql.cache.CachedRecord;
import org.elasql.schedule.tpart.sink.SunkPlan;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.sql.Constant;
import org.vanilladb.core.storage.tx.Transaction;
import org.vanilladb.core.util.Timers;

public class TPartTxLocalCache {

	private Transaction tx;
	private long txNum;
	private TPartCacheMgr cacheMgr;
	private Map<RecordKey, CachedRecord> recordCache = new HashMap<RecordKey, CachedRecord>();
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
	public CachedRecord readFromSink(RecordKey key, int mySinkId) {

		CachedRecord rec = null;
		Timers.getTimer().startComponentTimer("readFromSink");
		rec = cacheMgr.readFromSink(key, mySinkId, tx);
		rec.setSrcTxNum(txNum);
		recordCache.put(key, rec);

		Timers.getTimer().stopComponentTimer("readFromSink");

		return rec;
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
	public CachedRecord read(RecordKey key, long src) {
		CachedRecord rec = null;
		try {
			Timers.getTimer().startComponentTimer("read");
			rec = recordCache.get(key);
			if (rec != null)
				return rec;

			// if (src != txNum) {
			rec = cacheMgr.takeFromTx(key, src, txNum);
			recordCache.put(key, rec);
		} finally {
			Timers.getTimer().stopComponentTimer("read");
		}
		// }
		return rec;
	}

	public void update(RecordKey key, CachedRecord rec) {
		Timers.getTimer().startComponentTimer("update");
		rec.setSrcTxNum(txNum);
		recordCache.put(key, rec);
		Timers.getTimer().stopComponentTimer("update");

	}

	public void insert(RecordKey key, Map<String, Constant> fldVals) {
		Timers.getTimer().startComponentTimer("insert");
		CachedRecord rec = new CachedRecord(fldVals);
		rec.setSrcTxNum(txNum);
		rec.setNewInserted(true);
		recordCache.put(key, rec);
		Timers.getTimer().stopComponentTimer("insert");
	}

	public void delete(RecordKey key) {
		Timers.getTimer().startComponentTimer("delete");
		CachedRecord dummyRec = new CachedRecord();
		dummyRec.setSrcTxNum(txNum);
		dummyRec.delete();
		recordCache.put(key, dummyRec);
		Timers.getTimer().stopComponentTimer("delete");

	}

	public void senTothers(RecordKey key, CachedRecord rec, SunkPlan plan) {
		Timers.getTimer().startComponentTimer("senTothers");
		Long[] dests = plan.getWritingDestOfRecord(key);
		if (dests != null) {
			for (long dest : dests)
				cacheMgr.passToTheNextTx(key, rec, txNum, dest);

		}
		Timers.getTimer().stopComponentTimer("senTothers");
	}

	public void flush(SunkPlan plan) {
		Timers.getTimer().startComponentTimer("flush");
		// Pass to the transactions
		for (Map.Entry<RecordKey, CachedRecord> entry : recordCache.entrySet()) {
			Long[] dests = plan.getWritingDestOfRecord(entry.getKey());
			if (dests != null) {
				for (long dest : dests) {
					// The destination might be the local storage
					if (dest >= 0) {
						CachedRecord clonedRec = new CachedRecord(entry.getValue());
						cacheMgr.passToTheNextTx(entry.getKey(), clonedRec, txNum, dest);
					}
				}
			}
		}

		// Flush to the local storage (write back)
		for (RecordKey key : plan.getLocalWriteBackInfo()) {
			CachedRecord rec = recordCache.get(key);

			// If there is no such record in the local cache,
			// it might be pushed from the same transaction on the other
			// machine.
			if (rec == null)
				rec = cacheMgr.takeFromTx(key, txNum, localStorageId);

			cacheMgr.writeBack(key, plan.sinkProcessId(), rec, tx);

		}
		Timers.getTimer().stopComponentTimer("flush");
	}
}
