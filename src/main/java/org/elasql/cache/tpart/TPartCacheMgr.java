package org.elasql.cache.tpart;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.elasql.cache.CachedRecord;
import org.elasql.cache.RemoteRecordReceiver;
import org.elasql.remote.groupcomm.Tuple;
import org.elasql.sql.RecordKey;
import org.elasql.util.PeriodicalJob;
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

	private static WriteBackRecMgr writeBackMgr = new WriteBackRecMgr();

	private Map<CachedEntryKey, CachedRecord> exchange = new ConcurrentHashMap<CachedEntryKey, CachedRecord>();

	private final Object anchors[] = new Object[1009];

	public TPartCacheMgr() {
		for (int i = 0; i < anchors.length; ++i) {
			anchors[i] = new Object();
		}
		
		new PeriodicalJob(1000, 140000, new Runnable() {

			@Override
			public void run() {
				System.out.println("The size of exchange: " + exchange.size());
			}
			
		}).start();
	}

	private Object prepareAnchor(Object o) {
		int hash = o.hashCode() % anchors.length;
		if (hash < 0) {
			hash += anchors.length;
		}
		return anchors[0];
	}

	public CachedRecord readFromSink(RecordKey key, int mySinkId, Transaction tx) {
		return writeBackMgr.read(key, mySinkId, tx);
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

	public void passToTheNextTx(RecordKey key, CachedRecord rec, long src, long dest) {
		CachedEntryKey k = new CachedEntryKey(key, src, dest);
		synchronized (prepareAnchor(k)) {
			exchange.put(k, rec);
			prepareAnchor(k).notifyAll();
		}
	}

	@Override
	public void cacheRemoteRecord(Tuple t) {
		passToTheNextTx(t.key, t.rec, t.srcTxNum, t.destTxNum);
	}

	public void writeBack(RecordKey key, int sinkProcessId, CachedRecord rec, Transaction tx) {
		writeBackMgr.writeBackRecord(key, sinkProcessId, rec, tx);
	}

	public void setWriteBackInfo(RecordKey key, int sinkProcessId) {
		writeBackMgr.setWriteBackInfo(key, sinkProcessId);
	}
}
