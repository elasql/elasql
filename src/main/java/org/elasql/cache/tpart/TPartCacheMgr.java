package org.elasql.cache.tpart;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.RemoteRecordReceiver;
import org.elasql.remote.groupcomm.Tuple;
import org.elasql.sql.RecordKey;
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

	private static LocalStorageMgr localStorage = new LocalStorageMgr();

	private Map<CachedEntryKey, CachedRecord> exchange = new ConcurrentHashMap<CachedEntryKey, CachedRecord>();

	private final Object anchors[] = new Object[1009];

	public TPartCacheMgr() {
		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}
	}

	private Object prepareAnchor(Object o) {
		int hash = o.hashCode() % anchors.length;
		if (hash < 0) {
			hash += anchors.length;
		}
		return anchors[0];
	}

	public CachedRecord takeFromTx(RecordKey key, long src, long dest) {
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

	public void passToTheNextTx(RecordKey key, CachedRecord rec, long src, long dest, boolean isRemote) {
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
	
	public CachedRecord readFromSink(RecordKey key, Transaction tx) {
		return localStorage.read(key, tx);
	}
	
	public void writeBack(RecordKey key, CachedRecord rec, Transaction tx) {
		localStorage.writeBack(key, rec, tx);
	}
	
	public void registerSinkReading(RecordKey key, long txNum) {
		localStorage.requestSharedLock(key, txNum);
	}

	public void registerSinkWriteback(RecordKey key, long txNum) {
		localStorage.requestExclusiveLock(key, txNum);
	}
}
