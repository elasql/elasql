package org.elasql.cache.calvin;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.RemoteRecordReceiver;
import org.elasql.server.Elasql;
import org.elasql.sql.RecordKey;
import org.vanilladb.core.storage.tx.Transaction;

public class CalvinRemotePostOffice implements RemoteRecordReceiver {
	
	// TODO: add this to a properties file
	public static final int NUM_DISPATCHERS = 4;
	
	private RemoteRecordDispatcher[] dispatchers = new RemoteRecordDispatcher[NUM_DISPATCHERS];
	
	public CalvinRemotePostOffice() {
		for (int i = 0; i < NUM_DISPATCHERS; i++) {
			dispatchers[i] = new RemoteRecordDispatcher(i);
			Elasql.taskMgr().runTask(dispatchers[i]);
		}
	}
	
	public CalvinCacheMgr createCacheMgr(Transaction tx, boolean willHaveRemote) {
		CalvinCacheMgr cacheMgr = new CalvinCacheMgr(this, tx);
		
		if (willHaveRemote) {
			// Register this CacheMgr for remote records
			cacheMgr.createInboxForRemotes();
			registerCacheMgr(tx.getTransactionNumber(), cacheMgr);
		}
		
		return cacheMgr;
	}
	
	public void skipTransaction(long txNum) {
		int id = (int) (txNum % NUM_DISPATCHERS);
		dispatchers[id].ungisterTransaction(txNum);
	}

	public void cacheRemoteRecord(RecordKey key, CachedRecord rec) {
		int id = (int) (rec.getSrcTxNum() % NUM_DISPATCHERS);
		dispatchers[id].cacheRemoteRecord(key, rec);
	}

	void registerCacheMgr(long txNum, CalvinCacheMgr cacheMgr) {
		int id = (int) (txNum % NUM_DISPATCHERS);
		dispatchers[id].registerCacheMgr(txNum, cacheMgr);
	}

	void notifyTxCommitted(long txNum) {
		int id = (int) (txNum % NUM_DISPATCHERS);
		dispatchers[id].ungisterTransaction(txNum);
	}
}
